#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Provision isolated G11 cloud resources or run a bounded BigQuery query."""

import argparse
import datetime
import json
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class Cloud:
    def __init__(self):
        command = shutil.which("gcloud") or shutil.which("gcloud.cmd")
        if not command:
            raise RuntimeError("Install the Google Cloud SDK and configure application-default login")
        token = subprocess.run(
            [command, "auth", "application-default", "print-access-token"],
            capture_output=True, text=True, check=False)
        if token.returncode:
            raise RuntimeError("Application-default credentials need interactive reauthentication")
        self._token = token.stdout.strip()

    def request(self, method, url, body=None):
        headers = {"Authorization": "Bearer " + self._token}
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            content = json.load(error)
            raise RuntimeError(json.dumps({"http_status": error.code,
                                           "error": content.get("error")})) from None

    def query(self, project, location, sql, maximum_bytes):
        result = self.request("POST", "https://bigquery.googleapis.com/bigquery/v2/projects/"
                              + project + "/queries", {
                                  "query": sql, "useLegacySql": False,
                                  "location": location, "timeoutMs": 10000,
                                  "maximumBytesBilled": str(maximum_bytes)})
        while not result.get("jobComplete", True):
            reference = result["jobReference"]
            time.sleep(1)
            result = self.request(
                "GET", "https://bigquery.googleapis.com/bigquery/v2/projects/"
                + project + "/queries/" + reference["jobId"]
                + "?location=" + urllib.parse.quote(location))
        if result.get("errors"):
            raise RuntimeError(json.dumps({"jobReference": result.get("jobReference"),
                                           "errors": result["errors"]}))
        if result.get("pageToken"):
            raise RuntimeError("Result exceeds one page; narrow the manual verification query")
        return result


def provision(cloud, args):
    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = now.strftime("%Y%m%d_%H%M%S")
    connection_id = "g11_geography_" + run_id
    if args.reuse_connection:
        connection = cloud.request("GET", "https://bigqueryconnection.googleapis.com/v1/"
                                   + args.reuse_connection)
        connection_id = connection["name"].rsplit("/", 1)[-1]
    else:
        connection = cloud.request(
            "POST", "https://bigqueryconnection.googleapis.com/v1/projects/"
            + args.project + "/locations/" + args.location.lower()
            + "/connections?connectionId=" + connection_id,
            {"friendlyName": "G11 native Iceberg GEOGRAPHY verification", "cloudResource": {}})
    if args.create_bucket:
        cloud.request("POST", "https://storage.googleapis.com/storage/v1/b?project="
                      + args.project, {
                          "name": args.bucket, "location": args.location,
                          "labels": {"purpose": "g11-geography-validation"},
                          "iamConfiguration": {
                              "uniformBucketLevelAccess": {"enabled": True},
                              "publicAccessPrevention": "enforced"}})
    service_account = connection["cloudResource"]["serviceAccountId"]
    bucket_url = "https://storage.googleapis.com/storage/v1/b/" + args.bucket + "/iam"
    policy = cloud.request("GET", bucket_url + "?optionsRequestedPolicyVersion=3")
    expires = (now + datetime.timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    binding = {
        "role": "roles/storage.objectViewer",
        "members": ["serviceAccount:" + service_account],
        "condition": {
            "title": connection_id,
            "description": "Read only the G11 test prefix for seven days",
            "expression": "resource.name.startsWith('projects/_/buckets/" + args.bucket
                          + "/objects/g11/') && request.time < timestamp('" + expires + "')",
        },
    }
    policy.setdefault("bindings", []).append(binding)
    policy["version"] = 3
    cloud.request("PUT", bucket_url, policy)
    return {
        "project": args.project, "dataset": args.dataset, "location": args.location,
        "bucket": args.bucket, "run_id": run_id,
        "warehouse": "gs://" + args.bucket + "/g11/" + run_id + "/warehouse",
        "connection": connection["name"], "connection_service_account": service_account,
        "connection_read_binding": binding,
        "bigquery_table": args.project + "." + args.dataset + ".g11_geography_" + run_id,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["provision", "query"])
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", default="akvelon")
    parser.add_argument("--bucket")
    parser.add_argument("--create-bucket", action="store_true")
    parser.add_argument("--reuse-connection")
    parser.add_argument("--location", default="US")
    parser.add_argument("--sql", type=Path)
    parser.add_argument("--maximum-bytes", type=int, default=100000000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    cloud = Cloud()
    try:
        if args.command == "provision":
            if not args.bucket:
                parser.error("--bucket is required for provision")
            result = provision(cloud, args)
        else:
            if not args.sql:
                parser.error("--sql is required for query")
            result = cloud.query(args.project, args.location,
                                 args.sql.read_text(encoding="utf-8"), args.maximum_bytes)
    except RuntimeError as error:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"status": "FAILED", "error": str(error)}, indent=2)
                               + "\n", encoding="utf-8")
        raise
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "status": "RECORDED"}))


if __name__ == "__main__":
    main()
