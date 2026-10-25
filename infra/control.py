"""Control services added to the positively identified baseline of one environment."""

import json


from colvenna_isle.region import regional

def template(baseline, authorizer_id, artifact, source_commit, environment=None, pipelines=None, *, account):
    region = "${AWS::Region}"
    writer_function = baseline.get("WriterControlFunction", "")
    tags = [{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "control"},
            {"Key": "SourceCommit", "Value": source_commit}]
    resources = {
        "Logs": {"Type": "AWS::Logs::LogGroup", "Properties": {"RetentionInDays": 7, "Tags": tags}},
        "Role": {"Type": "AWS::IAM::Role", "Properties": {
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]},
            "Tags": tags, "Policies": [{"PolicyName": "SpaceMetadata", "PolicyDocument": {
                "Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                     "Resource": {"Fn::Sub": "arn:aws:logs:${AWS::Region}:${AWS::AccountId}:log-group:${Logs}:log-stream:*"}},
                    {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem",
                                                  "dynamodb:ConditionCheckItem"],
                     "Resource": f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}",
                     "Condition": {"ForAllValues:StringLike": {
                         "dynamodb:LeadingKeys": ["SPACE#*", "KEYHASH#*", "KEYID#*"]}}},
                    {"Effect": "Allow", "Action": ["dynamodb:GetItem"],
                     "Resource": f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}",
                     "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["WRITER#PRIMARY"]}}},
                    # Schedule updates maintain the scheduler index in the same transaction.
                    {"Effect": "Allow", "Action": ["dynamodb:PutItem", "dynamodb:DeleteItem"],
                     "Resource": f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}",
                     "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["PIPELINESCHEDULES#1"]}}},
                    {"Effect": "Allow", "Action": ["dynamodb:Query"],
                     "Resource": f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}",
                     "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["SPACE#*"]}}},
                    *([{"Effect": "Allow", "Action": ["lambda:InvokeFunction"],
                        "Resource": f"arn:aws:lambda:{region}:{account}:function:{writer_function}"}]
                      if writer_function else []),
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                     "Resource": f"arn:aws:s3:::{baseline['DataBucket']}/spaces/*/ingest/*"},
                    {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject", "s3:GetObjectVersion"],
                     "Resource": f"arn:aws:s3:::{baseline['DataBucket']}/spaces/*/pipeline-code/gen_*/*"},
                    {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObjectVersion"],
                     "Resource": f"arn:aws:s3:::{baseline['DataBucket']}/spaces/*/task-contexts/gen_*/*"},
                ]}}]}},
        "Control": {"Type": "AWS::Lambda::Function", "Properties": {
            "Runtime": "python3.12", "Architectures": ["arm64"], "Handler": "colvenna_isle.control.handler.handler",
            "Role": {"Fn::GetAtt": ["Role", "Arn"]}, "Timeout": 30, "MemorySize": 512,
            "Code": {"S3Bucket": baseline["DataBucket"], "S3Key": artifact["key"],
                     "S3ObjectVersion": artifact["version_id"]},
            "LoggingConfig": {"LogGroup": {"Ref": "Logs"}},
            "Environment": {"Variables": {"COLVENNA_ACCOUNT": account, "COLVENNA_CONTROL_TABLE": baseline["ControlTable"],
                                         "COLVENNA_DATA_BUCKET": baseline["DataBucket"],
                                         "COLVENNA_WRITER_CONTROL_FUNCTION": writer_function,
                                         "COLVENNA_WRITER_SOURCE_COMMIT": baseline["WriterSourceCommit"],
                                         "COLVENNA_SOURCE_COMMIT": source_commit}},
            "Tags": tags}},
        "Permission": {"Type": "AWS::Lambda::Permission", "Properties": {
            "FunctionName": {"Ref": "Control"}, "Action": "lambda:InvokeFunction",
            "Principal": "apigateway.amazonaws.com", "SourceAccount": account,
            "SourceArn": f"arn:aws:execute-api:{region}:{account}:{baseline['Api']}/*/*"}},
        "Integration": {"Type": "AWS::ApiGatewayV2::Integration", "Properties": {
            "ApiId": baseline["Api"], "IntegrationType": "AWS_PROXY",
            "IntegrationUri": {"Fn::GetAtt": ["Control", "Arn"]}, "PayloadFormatVersion": "2.0"}},
    }
    resources["AuthProbeLogs"] = {"Type": "AWS::Logs::LogGroup",
                                 "Properties": {"RetentionInDays": 7, "Tags": tags}}
    resources["AuthProbeRole"] = {"Type": "AWS::IAM::Role", "Properties": {
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]},
        "Tags": tags, "Policies": [{"PolicyName": "ReadAuthOnly", "PolicyDocument": {
            "Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": ["dynamodb:GetItem"],
                 "Resource": f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}"},
                {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                 "Resource": {"Fn::Sub": "arn:aws:logs:${AWS::Region}:${AWS::AccountId}:log-group:${AuthProbeLogs}:log-stream:*"}},
            ]}}]}}
    resources["AuthProbe"] = {"Type": "AWS::Lambda::Function", "Properties": {
        "Runtime": "python3.12", "Architectures": ["arm64"],
        "Handler": "colvenna_isle.control.authorizer.probe_handler",
        "Role": {"Fn::GetAtt": ["AuthProbeRole", "Arn"]}, "Timeout": 10, "MemorySize": 256,
        "Code": {"S3Bucket": baseline["DataBucket"], "S3Key": artifact["key"],
                 "S3ObjectVersion": artifact["version_id"]},
        "Environment": {"Variables": {"COLVENNA_CONTROL_TABLE": baseline["ControlTable"],
                                     "COLVENNA_SOURCE_COMMIT": source_commit}},
        "LoggingConfig": {"LogGroup": {"Ref": "AuthProbeLogs"}}, "Tags": tags}}
    if environment:
        project = environment["project"]
        policy = environment["policy"]
        variables = resources["Control"]["Properties"]["Environment"]["Variables"]
        variables.update(COLVENNA_ENVIRONMENT_PROJECT=project,
                         COLVENNA_ENVIRONMENT_POLICY=json.dumps(policy, separators=(",", ":")))
        statements = resources["Role"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
        statements.extend([
            {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem"],
             "Resource": f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}",
             "Condition": {"ForAllValues:StringLike": {
                 "dynamodb:LeadingKeys": ["ENVIRONMENT#1#*", "ENVIRONMENTBUILDS#1"]}}},
            {"Effect": "Allow", "Action": ["codebuild:StartBuild"],
             "Resource": f"arn:aws:codebuild:{region}:{account}:project/{project}"},
            {"Effect": "Allow", "Action": ["codebuild:BatchGetBuilds"],
             "Resource": [
                 f"arn:aws:codebuild:{region}:{account}:project/{project}",
                 f"arn:aws:codebuild:{region}:{account}:build/{project}:*"]},
            {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObjectVersion"],
             "Resource": f"arn:aws:s3:::{baseline['DataBucket']}/spaces/*/environment-builds/gen_*/*"},
            {"Effect": "Allow", "Action": ["s3:ListBucketVersions"],
             "Resource": f"arn:aws:s3:::{baseline['DataBucket']}",
             "Condition": {"StringLike": {"s3:prefix": ["spaces/*/environment-builds/gen_*/*"]}}},
            {"Effect": "Allow", "Action": ["ecr:BatchGetImage", "ecr:DescribeImages",
                                         "ecr:GetDownloadUrlForLayer"],
             "Resource": f"arn:aws:ecr:{region}:{account}:repository/{policy['repository']}"},
            {"Effect": "Allow", "Action": ["lambda:GetFunction", "lambda:GetFunctionEventInvokeConfig"],
             "Resource": f"arn:aws:lambda:{region}:{account}:function:{policy['runtime_prefix']}*:*"},
            # ECS authorizes DescribeTaskDefinition against *, not a task ARN.
            # The verifier still checks the owned family and numeric revision.
            {"Effect": "Allow", "Action": ["ecs:DescribeTaskDefinition"],
             "Resource": "*"},
        ])
    routes = []
    if environment and pipelines:
        add_pipeline_runs(resources, baseline, artifact, source_commit, environment, pipelines, tags,
                          account, region, writer_function)
        add_governance(resources, baseline, artifact, source_commit, tags, account, region, writer_function)
        routes = [
            ("RunCreate", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs"),
            ("RunStatus", "GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}"),
            ("RunCancel", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/cancel"),
            ("RunRetry", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/retry"),
        ] + GOVERNANCE_ROUTES
    for name, key in routes + [
        ("ReadConfiguration", "GET /v1/admin/spaces/{space_id}/configuration"),
        ("RegisterDataset", "POST /v1/admin/spaces/{space_id}/datasets"),
        ("ListKeys", "GET /v1/admin/spaces/{space_id}/api-keys"),
        ("CreateKey", "POST /v1/admin/spaces/{space_id}/api-keys"),
        ("RevokeKey", "POST /v1/admin/spaces/{space_id}/api-keys/{key_id}/revoke"),
        ("RotateKey", "POST /v1/admin/spaces/{space_id}/api-keys/{key_id}/rotate"),
        ("IngestionStatus", "GET /v1/spaces/{space_id}/ingestions/{ingestion_id}"),
        ("CreateEnvironment", "POST /v1/spaces/{space_id}/runtime-environments"),
        ("BuildEnvironment", "POST /v1/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}/build"),
        ("EnvironmentStatus", "GET /v1/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}"),
        ("CreatePipeline", "POST /v1/spaces/{space_id}/pipelines"),
        ("PipelineMetadata", "GET /v1/spaces/{space_id}/pipelines/{pipeline_id}"),
        ("PipelineDraft", "PUT /v1/spaces/{space_id}/pipelines/{pipeline_id}/draft"),
        ("PipelineUpload", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/artifact-uploads"),
        ("PipelineValidate", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/validate"),
        ("PipelinePublish", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/versions"),
        ("PipelineVersion", "GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/versions/{pipeline_version}"),
        ("PipelineActivate", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/activate"),
        ("PipelineSchedule", "PUT /v1/spaces/{space_id}/pipelines/{pipeline_id}/schedule"),
    ]:
        resources[name] = {"Type": "AWS::ApiGatewayV2::Route", "Properties": {
            "ApiId": baseline["Api"], "RouteKey": key, "AuthorizationType": "CUSTOM",
            "AuthorizerId": authorizer_id, "Target": {"Fn::Sub": "integrations/${Integration}"}}}
    for resource in resources.values():
        if resource["Type"] == "AWS::Lambda::Function":
            resource["Properties"].setdefault("Environment", {}).setdefault("Variables", {})["COLVENNA_REGION"] = {"Ref": "AWS::Region"}
    return regional({
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Colvenna Isle control services on the private baseline",
        "Resources": resources,
        "Outputs": {
            **({"SchedulerFunction": {"Value": {"Ref": "Scheduler"}},
                "SchedulerLogs": {"Value": {"Ref": "SchedulerLogs"}},
                "CollectorFunction": {"Value": {"Ref": "Collector"}},
                "CollectorLogs": {"Value": {"Ref": "CollectorLogs"}},
                "CollectorRoleArn": {"Value": {"Fn::GetAtt": ["CollectorRole", "Arn"]}},
                "ComputeSecurityGroup": {"Value": {"Ref": "ComputeSecurityGroup"}}}
               if environment and pipelines else {}),
            "ControlFunction": {"Value": {"Ref": "Control"}},
            "ControlLogs": {"Value": {"Ref": "Logs"}},
            "AuthProbeFunction": {"Value": {"Ref": "AuthProbe"}},
            "AuthProbeLogs": {"Value": {"Ref": "AuthProbeLogs"}},
            "SourceCommit": {"Value": source_commit},
            "BaselineStackId": {"Value": baseline["StackId"]},
            **{name: {"Value": baseline[name]} for name in ["ControlTable", "DataBucket", "Api", "ApiUrl"]},
        },
    })


# Console, audit, diagnostics, quotas, costs and retention routes on the control integration.
GOVERNANCE_ROUTES = [
    ("ListPipelines", "GET /v1/spaces/{space_id}/pipelines"),
    ("ListPipelineVersions", "GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/versions"),
    ("DraftArtifact", "GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/draft/artifacts"),
    ("InlineArtifact", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/artifacts"),
    ("ListRuns", "GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs"),
    ("TrialRun", "POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/trial-runs"),
    ("RunPreview", "GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/preview"),
    ("ListEnvironments", "GET /v1/spaces/{space_id}/runtime-environments"),
    ("AdminStatus", "GET /v1/admin/spaces/{space_id}/status"),
    ("AdminMetrics", "GET /v1/admin/spaces/{space_id}/metrics"),
    ("AdminTrace", "GET /v1/admin/spaces/{space_id}/trace"),
    ("AdminAudit", "GET /v1/admin/spaces/{space_id}/audit"),
    ("AdminQuotas", "GET /v1/admin/spaces/{space_id}/quotas"),
    ("AdminCosts", "GET /v1/admin/spaces/{space_id}/costs"),
    ("RetentionStatus", "GET /v1/admin/spaces/{space_id}/retention"),
    ("RetentionUpdate", "PUT /v1/admin/spaces/{space_id}/retention"),
    ("RetentionHold", "POST /v1/admin/spaces/{space_id}/retention/holds"),
    ("RetentionHoldRelease", "POST /v1/admin/spaces/{space_id}/retention/holds/{hold_id}/release"),
    ("RetentionCollect", "POST /v1/admin/spaces/{space_id}/retention/collections"),
    ("RetentionCollection", "GET /v1/admin/spaces/{space_id}/retention/collections/{collection_id}"),
]
# Collected object categories; SlateDB database prefixes (gen_*/warehouse) are never included.
COLLECTED_PREFIXES = ["ingest", "inputs", "pipeline-code", "environment-builds", "staging"]


def queue_arn(url, account, region):
    return f"arn:aws:sqs:{region}:{account}:{url.rsplit('/', 1)[-1]}"


def add_governance(resources, baseline, artifact, source_commit, tags, account, region, writer_function):
    """Admin reads for the control API and the retention collector with exact-version delete only."""
    table = f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}"
    bucket = baseline["DataBucket"]
    queues = [queue_arn(baseline[name], account, region) for name in ["IngressQueue", "Dlq"]]
    variables = resources["Control"]["Properties"]["Environment"]["Variables"]
    variables.update(COLVENNA_INGRESS_QUEUE=baseline["IngressQueue"], COLVENNA_DLQ=baseline["Dlq"],
                     COLVENNA_COLLECTOR_FUNCTION={"Ref": "Collector"})
    # A separate managed policy keeps the inline control policy within IAM size limits.
    resources["GovernancePolicy"] = {"Type": "AWS::IAM::ManagedPolicy", "Properties": {
        "Roles": [{"Ref": "Role"}], "PolicyDocument": {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["dynamodb:PutItem", "dynamodb:Query", "dynamodb:GetItem"],
             "Resource": table, "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["AUDIT#*"]}}},
            {"Effect": "Allow", "Action": ["dynamodb:UpdateItem", "dynamodb:DeleteItem"], "Resource": table,
             "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["SPACE#*"]}}},
            {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:Query"], "Resource": table,
             "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["QUERY#*", "WRITERPROCESS#*"]}}},
            {"Effect": "Allow", "Action": ["dynamodb:PutItem", "dynamodb:DeleteItem"], "Resource": table,
             "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": [
                 "RETENTIONQUEUE#1", "RETENTIONSPACES#1"]}}},
            {"Effect": "Allow", "Action": ["sqs:GetQueueAttributes"], "Resource": queues},
            {"Effect": "Allow", "Action": ["cloudwatch:GetMetricStatistics"], "Resource": "*"},
            {"Effect": "Allow", "Action": ["s3:GetObjectVersion"], "Resource": [
                f"arn:aws:s3:::{bucket}/spaces/*/staging/gen_*/*",
                f"arn:aws:s3:::{bucket}/spaces/*/query-intents/gen_*/*"]},
            {"Effect": "Allow", "Action": ["lambda:InvokeFunction"], "Resource": {"Fn::GetAtt": ["Collector", "Arn"]}},
        ]}}}
    resources["CollectorLogs"] = {"Type": "AWS::Logs::LogGroup", "Properties": {"RetentionInDays": 7, "Tags": tags}}
    objects = [f"arn:aws:s3:::{bucket}/spaces/*/{name}/gen_*/*" for name in COLLECTED_PREFIXES]
    resources["CollectorRole"] = {"Type": "AWS::IAM::Role", "Properties": {
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]},
        "Tags": tags, "Policies": [{"PolicyName": "RetentionCollector", "PolicyDocument": {
            "Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                 "Resource": {"Fn::Sub": f"arn:aws:logs:{region}:{account}:log-group:${{CollectorLogs}}:log-stream:*"}},
                {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
                                              "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:ConditionCheckItem"],
                 "Resource": table, "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": [
                     "SPACE#*", "AUDIT#*", "RETENTIONQUEUE#1", "RETENTIONSPACES#1", "RETENTIONLEASE#*"]}}},
                {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:Query"], "Resource": table,
                 "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": [
                     "KEYHASH#*", "KEYID#*", "WRITER#PRIMARY", "PIPELINERUNS#1", "ENVIRONMENT#1#*"]}}},
                {"Effect": "Allow", "Action": ["lambda:InvokeFunction"],
                 "Resource": f"arn:aws:lambda:{region}:{account}:function:{writer_function}"},
                {"Effect": "Allow", "Action": ["sqs:GetQueueAttributes"], "Resource": queues},
                {"Effect": "Allow", "Action": ["cloudwatch:GetMetricStatistics"], "Resource": "*"},
                {"Effect": "Allow", "Action": ["s3:ListBucketVersions"], "Resource": f"arn:aws:s3:::{bucket}",
                 "Condition": {"StringLike": {"s3:prefix": [f"spaces/*/{name}/gen_*/*" for name in
                                                            COLLECTED_PREFIXES + ["ingestion-replay", "retention"]]}}},
                {"Effect": "Allow", "Action": ["s3:GetObjectVersion", "s3:DeleteObjectVersion"], "Resource": objects},
                {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObjectVersion"],
                 "Resource": f"arn:aws:s3:::{bucket}/spaces/*/retention/gen_*/*"},
                # Explicit: the collector can never delete or overwrite a SlateDB database object.
                {"Effect": "Deny", "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion", "s3:PutObject"],
                 "Resource": f"arn:aws:s3:::{bucket}/spaces/*/gen_*/warehouse/*"},
            ]}}]}}
    resources["Collector"] = {"Type": "AWS::Lambda::Function", "Properties": {
        "Runtime": "python3.12", "Architectures": ["arm64"], "Handler": "colvenna_isle.control.collector.handler",
        "Role": {"Fn::GetAtt": ["CollectorRole", "Arn"]}, "Timeout": 300, "MemorySize": 512,
        # One collector at a time; a per-space lease also serializes overlapping work.
        "ReservedConcurrentExecutions": 1,
        "Code": {"S3Bucket": baseline["DataBucket"], "S3Key": artifact["key"], "S3ObjectVersion": artifact["version_id"]},
        "LoggingConfig": {"LogGroup": {"Ref": "CollectorLogs"}},
        "Environment": {"Variables": {
            "COLVENNA_ACCOUNT": account, "COLVENNA_CONTROL_TABLE": baseline["ControlTable"],
            "COLVENNA_DATA_BUCKET": baseline["DataBucket"],
            "COLVENNA_WRITER_CONTROL_FUNCTION": writer_function,
            "COLVENNA_WRITER_SOURCE_COMMIT": baseline["WriterSourceCommit"], "COLVENNA_SOURCE_COMMIT": source_commit,
            "COLVENNA_INGRESS_QUEUE": baseline["IngressQueue"], "COLVENNA_DLQ": baseline["Dlq"]}},
        "Tags": tags}}
    resources["CollectorInvokeRole"] = {"Type": "AWS::IAM::Role", "Properties": {
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "scheduler.amazonaws.com"}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:SourceAccount": account}}}]},
        "Tags": tags, "Policies": [{"PolicyName": "InvokeCollector", "PolicyDocument": {
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction",
                                                   "Resource": {"Fn::GetAtt": ["Collector", "Arn"]}}]}}]}}
    resources["CollectorTick"] = {"Type": "AWS::Scheduler::Schedule", "Properties": {
        "Description": "Colvenna Isle retention: hourly sweeps for enabled spaces and queued collection requests",
        "ScheduleExpression": "rate(5 minutes)", "FlexibleTimeWindow": {"Mode": "OFF"}, "State": "ENABLED",
        "Target": {"Arn": {"Fn::GetAtt": ["Collector", "Arn"]},
                   "RoleArn": {"Fn::GetAtt": ["CollectorInvokeRole", "Arn"]},
                   "Input": json.dumps({"source": "colvenna.retention"}),
                   "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 300}}}}


def add_pipeline_runs(resources, baseline, artifact, source_commit, environment, pipelines, tags,
                      account, region, writer_function):
    """Run API, the minute scheduler and independent compute dispatch permissions."""
    table = f"arn:aws:dynamodb:{region}:{account}:table/{baseline['ControlTable']}"
    bucket = baseline["DataBucket"]
    cluster = f"arn:aws:ecs:{region}:{account}:cluster/{baseline['Cluster']}"
    policy = environment["policy"]
    queue = policy["notification_queue_arn"]
    resources["ComputeSecurityGroup"] = {"Type": "AWS::EC2::SecurityGroup", "Properties": {
        "GroupDescription": "Colvenna Isle independent compute tasks; egress only, no inbound listener",
        "VpcId": baseline["Vpc"], "Tags": tags}}
    network = {"cluster": baseline["Cluster"], "subnets": [baseline["PrivateSubnet1"], baseline["PrivateSubnet2"]],
               "security_groups": ["__COMPUTE_SECURITY_GROUP__"]}
    network_value = {"Fn::Sub": [json.dumps(network, separators=(",", ":")).replace(
        "__COMPUTE_SECURITY_GROUP__", "${Group}"), {"Group": {"Ref": "ComputeSecurityGroup"}}]}
    # Run admission writes its audit record in the same transaction (scheduled runs included).
    keys = ["SPACE#*", "KEYHASH#*", "KEYID#*", "PIPELINERUNS#1", "PIPELINESCHEDULES#1", "PIPELINEFAULTS#1", "AUDIT#*"]
    run_statements = [
        {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem",
                                      "dynamodb:ConditionCheckItem", "dynamodb:Query", "dynamodb:UpdateItem"],
         "Resource": table, "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": keys}}},
        {"Effect": "Allow", "Action": ["dynamodb:GetItem"], "Resource": table,
         "Condition": {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["WRITER#PRIMARY", "ENVIRONMENT#1#*"]}}},
        {"Effect": "Allow", "Action": ["ecs:StopTask", "ecs:DescribeTasks"],
         "Resource": f"arn:aws:ecs:{region}:{account}:task/{baseline['Cluster']}/*"},
    ]
    statements = resources["Role"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    statements.extend(run_statements + [
        {"Effect": "Allow", "Action": ["lambda:InvokeFunction"], "Resource": {"Fn::GetAtt": ["Scheduler", "Arn"]}}])
    variables = resources["Control"]["Properties"]["Environment"]["Variables"]
    variables.update(COLVENNA_COMPUTE_NETWORK=network_value, COLVENNA_SCHEDULER_FUNCTION={"Ref": "Scheduler"})
    resources["SchedulerLogs"] = {"Type": "AWS::Logs::LogGroup", "Properties": {"RetentionInDays": 7, "Tags": tags}}
    resources["SchedulerRole"] = {"Type": "AWS::IAM::Role", "Properties": {
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]},
        "Tags": tags, "Policies": [{"PolicyName": "PipelineScheduler", "PolicyDocument": {
            "Version": "2012-10-17", "Statement": run_statements + [
                {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                 "Resource": {"Fn::Sub": f"arn:aws:logs:{region}:{account}:log-group:${{SchedulerLogs}}:log-stream:*"}},
                {"Effect": "Allow", "Action": ["lambda:InvokeFunction"],
                 "Resource": [f"arn:aws:lambda:{region}:{account}:function:{writer_function}",
                              f"arn:aws:lambda:{region}:{account}:function:{policy['runtime_prefix']}*:*"]},
                {"Effect": "Allow", "Action": ["ecs:RunTask"],
                 "Resource": f"arn:aws:ecs:{region}:{account}:task-definition/{policy['runtime_prefix']}*:*",
                 "Condition": {"ArnEquals": {"ecs:cluster": cluster}}},
                {"Effect": "Allow", "Action": ["ecs:TagResource"],
                 "Resource": f"arn:aws:ecs:{region}:{account}:task/{baseline['Cluster']}/*",
                 "Condition": {"StringEquals": {"ecs:CreateAction": "RunTask"}}},
                {"Effect": "Allow", "Action": ["ecs:ListTasks"], "Resource": "*",
                 "Condition": {"ArnEquals": {"ecs:cluster": cluster}}},
                {"Effect": "Allow", "Action": ["iam:PassRole"],
                 "Resource": [f"arn:aws:iam::{account}:role/{policy['runtime_prefix']}*", policy["execution_role_arn"]],
                 "Condition": {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}},
                {"Effect": "Allow", "Action": ["s3:GetObject", "s3:GetObjectVersion"],
                 "Resource": [f"arn:aws:s3:::{bucket}/spaces/*/pipeline-code/gen_*/*",
                              f"arn:aws:s3:::{bucket}/spaces/*/staging/gen_*/*"]},
                {"Effect": "Allow", "Action": ["s3:PutObject", "s3:GetObject", "s3:GetObjectVersion"],
                 "Resource": f"arn:aws:s3:::{bucket}/spaces/*/task-contexts/gen_*/*"},
                {"Effect": "Allow", "Action": ["s3:ListBucketVersions", "s3:ListBucket"],
                 "Resource": f"arn:aws:s3:::{bucket}",
                 "Condition": {"StringLike": {"s3:prefix": ["spaces/*/staging/gen_*/*",
                                                            "spaces/*/task-contexts/gen_*/*"]}}},
                {"Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage",
                                              "sqs:GetQueueAttributes"], "Resource": queue},
            ]}}]}}
    resources["Scheduler"] = {"Type": "AWS::Lambda::Function", "Properties": {
        "Runtime": "python3.12", "Architectures": ["arm64"], "Handler": "colvenna_isle.control.scheduler.handler",
        "Role": {"Fn::GetAtt": ["SchedulerRole", "Arn"]}, "Timeout": 90, "MemorySize": 512,
        # One tick at a time; every step is also conditional on run revision.
        "ReservedConcurrentExecutions": 1,
        "Code": {"S3Bucket": baseline["DataBucket"], "S3Key": artifact["key"], "S3ObjectVersion": artifact["version_id"]},
        "LoggingConfig": {"LogGroup": {"Ref": "SchedulerLogs"}},
        "Environment": {"Variables": {
            "COLVENNA_ACCOUNT": account, "COLVENNA_CONTROL_TABLE": baseline["ControlTable"],
            "COLVENNA_DATA_BUCKET": baseline["DataBucket"],
            "COLVENNA_WRITER_CONTROL_FUNCTION": writer_function,
            "COLVENNA_WRITER_SOURCE_COMMIT": baseline["WriterSourceCommit"], "COLVENNA_SOURCE_COMMIT": source_commit,
            "COLVENNA_ENVIRONMENT_PROJECT": environment["project"],
            "COLVENNA_ENVIRONMENT_POLICY": json.dumps(policy, separators=(",", ":")),
            "COLVENNA_COMPUTE_NETWORK": network_value, "COLVENNA_PIPELINE_EVENTS_QUEUE": policy["notification_queue_url"]}},
        "Tags": tags}}
    resources["SchedulerInvokeRole"] = {"Type": "AWS::IAM::Role", "Properties": {
        "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Principal": {"Service": "scheduler.amazonaws.com"}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:SourceAccount": account}}}]},
        "Tags": tags, "Policies": [{"PolicyName": "InvokeScheduler", "PolicyDocument": {
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "lambda:InvokeFunction",
                                                   "Resource": {"Fn::GetAtt": ["Scheduler", "Arn"]}}]}}]}}
    resources["SchedulerTick"] = {"Type": "AWS::Scheduler::Schedule", "Properties": {
        "Description": "Colvenna Isle pipeline scheduler: minute-level triggers and reconciliation, not a sub-minute SLA",
        "ScheduleExpression": "rate(1 minute)", "FlexibleTimeWindow": {"Mode": "OFF"},
        "State": pipelines.get("state", "ENABLED"),
        "Target": {"Arn": {"Fn::GetAtt": ["Scheduler", "Arn"]},
                   "RoleArn": {"Fn::GetAtt": ["SchedulerInvokeRole", "Arn"]},
                   "Input": json.dumps({"source": "colvenna.schedule"}),
                   "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 60}}}}
