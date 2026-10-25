#!/usr/bin/env python3
"""Environment baseline stack: private network, storage and compute, the public API and the browser assets."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from colvenna_isle.region import regional


def ref(name):
    return {"Ref": name}


def get(name, attribute):
    return {"Fn::GetAtt": [name, attribute]}


def sub(value):
    return {"Fn::Sub": value}


# Static browser objects served through CloudFront OAC (Query Editor and Admin Console).
WEB_FILES = ["index.html", "query-editor/app.js", "query-editor/app.css",
             "admin.html", "admin/app.js", "admin/app.css"]


def template():
    resources = {}
    def add(name, kind, **properties):
        resources[name] = {"Type": kind, "Properties": properties}
        return ref(name)

    parameters = {
        "SourceCommit": {"Type": "String", "AllowedPattern": "[0-9a-f]{40}"},
        "StorageImage": {"Type": "String", "AllowedPattern":
                         "[0-9]{12}.dkr.ecr.[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*.amazonaws.com/colvenna-isle-images@sha256:[0-9a-f]{64}"},
        "QueryDesired": {"Type": "Number", "Default": 0, "AllowedValues": [0, 1, 2, 3, 4]},
        "QueryImage": {"Type": "String", "Default": "", "AllowedPattern":
                       "(|[0-9]{12}.dkr.ecr.[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*.amazonaws.com/colvenna-isle-images@sha256:[0-9a-f]{64})"},
        "QuerySourceCommit": {"Type": "String", "Default": "", "AllowedPattern": "(|[0-9a-f]{40})"},
        "BrowserSourceCommit": {"Type": "String", "Default": "", "AllowedPattern": "(|[0-9a-f]{40})"},
        "AuthCodeBucket": {"Type": "String", "Default": ""},
        "AuthCodeKey": {"Type": "String", "Default": ""},
        "AuthCodeVersion": {"Type": "String", "Default": ""},
        "AuthSourceCommit": {"Type": "String", "Default": "", "AllowedPattern": "(|[0-9a-f]{40})"},
        "AuthEnabled": {"Type": "String", "Default": "false", "AllowedValues": ["false", "true"]},
        "IntakeCodeBucket": {"Type": "String", "Default": ""},
        "IntakeCodeKey": {"Type": "String", "Default": ""},
        "IntakeCodeVersion": {"Type": "String", "Default": ""},
        "IntakeSourceCommit": {"Type": "String", "Default": "", "AllowedPattern": "(|[0-9a-f]{40})"},
        "IntakeEnabled": {"Type": "String", "Default": "false", "AllowedValues": ["false", "true"]},
        "SyncCodeBucket": {"Type": "String", "Default": ""},
        "SyncCodeKey": {"Type": "String", "Default": ""},
        "SyncCodeVersion": {"Type": "String", "Default": ""},
        "SyncSourceCommit": {"Type": "String", "Default": "", "AllowedPattern": "(|[0-9a-f]{40})"},
        "SyncEnabled": {"Type": "String", "Default": "false", "AllowedValues": ["false", "true"]},
        "WriterImage": {"Type": "String", "Default": "", "AllowedPattern":
                        "(|[0-9]{12}.dkr.ecr.[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*.amazonaws.com/colvenna-isle-images@sha256:[0-9a-f]{64})"},
        "WriterSourceCommit": {"Type": "String", "Default": "", "AllowedPattern": "(|[0-9a-f]{40})"},
        "WriterEnvironmentPolicy": {"Type": "String", "Default": "{}"},
        "WriterCodeBucket": {"Type": "String", "Default": ""},
        "WriterCodeKey": {"Type": "String", "Default": ""},
        "WriterCodeVersion": {"Type": "String", "Default": ""},
        "WriterProbeSpace": {"Type": "String", "Default": "", "AllowedPattern": "(|probe_[a-z0-9_]+)"},
        "WriterProbeGeneration": {"Type": "String", "Default": "", "AllowedPattern": "(|gen_[0-9a-f]{32})"},
        # Per-request segment timing lines; "off" measures the instrumentation overhead.
        "Timing": {"Type": "String", "Default": "on", "AllowedValues": ["on", "off"]},
        # The account-level backup vault (infra/backups.py); empty grants no backup access.
        "BackupBucket": {"Type": "String", "Default": "", "AllowedPattern": "(|[a-z0-9][a-z0-9.-]{1,61}[a-z0-9])"},
    }
    tags = [{"Key": "Project", "Value": "colvenna-isle"}, {"Key": "Component", "Value": "core"},
            {"Key": "SourceCommit", "Value": ref("SourceCommit")}]
    add("Vpc", "AWS::EC2::VPC", CidrBlock="10.87.0.0/16", EnableDnsSupport=True,
        EnableDnsHostnames=True, Tags=tags)
    add("InternetGateway", "AWS::EC2::InternetGateway", Tags=tags)
    add("GatewayAttachment", "AWS::EC2::VPCGatewayAttachment", VpcId=ref("Vpc"),
        InternetGatewayId=ref("InternetGateway"))
    for index in [1, 2]:
        zone = {"Fn::Select": [index - 1, {"Fn::GetAZs": {"Ref": "AWS::Region"}}]}
        add(f"PublicSubnet{index}", "AWS::EC2::Subnet", VpcId=ref("Vpc"), AvailabilityZone=zone,
            CidrBlock=f"10.87.{index}.0/24", MapPublicIpOnLaunch=False, Tags=tags)
        add(f"PrivateSubnet{index}", "AWS::EC2::Subnet", VpcId=ref("Vpc"), AvailabilityZone=zone,
            CidrBlock=f"10.87.{index+10}.0/24", MapPublicIpOnLaunch=False, Tags=tags)
    add("PublicRoutes", "AWS::EC2::RouteTable", VpcId=ref("Vpc"), Tags=tags)
    add("InternetRoute", "AWS::EC2::Route", RouteTableId=ref("PublicRoutes"),
        DestinationCidrBlock="0.0.0.0/0", GatewayId=ref("InternetGateway"))
    resources["InternetRoute"]["DependsOn"] = "GatewayAttachment"
    add("NatAddress", "AWS::EC2::EIP", Domain="vpc", Tags=tags)
    add("Nat", "AWS::EC2::NatGateway", AllocationId=get("NatAddress", "AllocationId"),
        SubnetId=ref("PublicSubnet1"), Tags=tags)
    resources["Nat"]["DependsOn"] = "GatewayAttachment"
    add("PrivateRoutes", "AWS::EC2::RouteTable", VpcId=ref("Vpc"), Tags=tags)
    add("NatRoute", "AWS::EC2::Route", RouteTableId=ref("PrivateRoutes"),
        DestinationCidrBlock="0.0.0.0/0", NatGatewayId=ref("Nat"))
    for index in [1, 2]:
        for kind in ["Public", "Private"]:
            add(f"{kind}Association{index}", "AWS::EC2::SubnetRouteTableAssociation",
                SubnetId=ref(f"{kind}Subnet{index}"), RouteTableId=ref(f"{kind}Routes"))
    for service in ["s3", "dynamodb"]:
        add(service.capitalize()+"Endpoint", "AWS::EC2::VPCEndpoint", VpcId=ref("Vpc"),
            ServiceName=f"com.amazonaws.${{AWS::Region}}.{service}", VpcEndpointType="Gateway",
            RouteTableIds=[ref("PrivateRoutes")])
    for name, description in [
        ("LinkSecurityGroup", "API Gateway VPC Link; no ingress"),
        ("AlbSecurityGroup", "Internal ALB; only API Gateway VPC Link"),
        ("QuerySecurityGroup", "Private query baseline; only internal ALB"),
        ("WriterSecurityGroup", "Private writer; separate control/query and synchronous proxy ports"),
        ("WriterControlSecurityGroup", "IAM writer control Lambda; no ingress"),
        ("SyncSecurityGroup", "Synchronous VPC Lambda; no ingress"),
    ]:
        add(name, "AWS::EC2::SecurityGroup", GroupDescription=description, VpcId=ref("Vpc"),
            SecurityGroupEgress=[{"IpProtocol": "-1", "CidrIp": "0.0.0.0/0"}], Tags=tags)
    add("LinkToAlb", "AWS::EC2::SecurityGroupIngress", GroupId=ref("AlbSecurityGroup"),
        IpProtocol="tcp", FromPort=80, ToPort=80, SourceSecurityGroupId=ref("LinkSecurityGroup"))
    add("AlbToQuery", "AWS::EC2::SecurityGroupIngress", GroupId=ref("QuerySecurityGroup"),
        IpProtocol="tcp", FromPort=8080, ToPort=8080, SourceSecurityGroupId=ref("AlbSecurityGroup"))
    for caller in ["WriterControlSecurityGroup", "QuerySecurityGroup"]:
        add(caller + "ToWriter", "AWS::EC2::SecurityGroupIngress", GroupId=ref("WriterSecurityGroup"),
            IpProtocol="tcp", FromPort=8081, ToPort=8081, SourceSecurityGroupId=ref(caller))
    add("SyncToWriter", "AWS::EC2::SecurityGroupIngress", GroupId=ref("WriterSecurityGroup"),
        IpProtocol="tcp", FromPort=8082, ToPort=8082, SourceSecurityGroupId=ref("SyncSecurityGroup"))
    for name in ["DataBucket", "WebBucket"]:
        add(name, "AWS::S3::Bucket", PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True},
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
            BucketEncryption={"ServerSideEncryptionConfiguration": [
                {"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]},
            VersioningConfiguration={"Status": "Enabled"}, Tags=tags)
    def tls_statement(bucket):
        return {"Effect": "Deny", "Principal": "*", "Action": "s3:*",
                "Resource": [get(bucket, "Arn"), sub("${"+bucket+".Arn}/*")],
                "Condition": {"Bool": {"aws:SecureTransport": "false"}}}
    add("DataPolicy", "AWS::S3::BucketPolicy", Bucket=ref("DataBucket"),
        PolicyDocument={"Version": "2012-10-17", "Statement": [tls_statement("DataBucket")]})
    add("ControlTable", "AWS::DynamoDB::Table", BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"} for key in ["PK", "SK"]],
        KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "SK", "KeyType": "RANGE"}],
        SSESpecification={"SSEEnabled": True}, PointInTimeRecoverySpecification={
            "PointInTimeRecoveryEnabled": True}, Tags=tags)
    for name, retention in [("Dlq", 1209600), ("IngressQueue", 604800)]:
        props = {"FifoQueue": True, "ContentBasedDeduplication": False,
                 "SqsManagedSseEnabled": True, "MessageRetentionPeriod": retention,
                 "VisibilityTimeout": 120, "Tags": tags}
        if name == "IngressQueue":
            props["RedrivePolicy"] = {"deadLetterTargetArn": get("Dlq", "Arn"), "maxReceiveCount": 5}
        add(name, "AWS::SQS::Queue", **props)
    for name in ["AuthorizerLogs", "IntakeLogs", "SyncLogs", "ApiLogs", "TaskLogs"]:
        add(name, "AWS::Logs::LogGroup", RetentionInDays=7, Tags=tags)
    def role(name, service, statements):
        return add(name, "AWS::IAM::Role",
                   AssumeRolePolicyDocument={"Version": "2012-10-17", "Statement": [{
                       "Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"}]},
                   Policies=[{"PolicyName": "CurrentBaseline", "PolicyDocument": {
                       "Version": "2012-10-17", "Statement": statements}}] if statements else [], Tags=tags)
    def allow(actions, resource, condition=None):
        statement = {"Effect": "Allow", "Action": actions, "Resource": resource}
        if condition:
            statement["Condition"] = condition
        return statement
    log_allow = lambda name: allow(["logs:CreateLogStream", "logs:PutLogEvents"],
                                  sub("arn:aws:logs:${AWS::Region}:${AWS::AccountId}:log-group:${"+name+"}:log-stream:*"))
    role("AuthorizerRole", "lambda.amazonaws.com", [
        log_allow("AuthorizerLogs"), allow(["dynamodb:GetItem"], get("ControlTable", "Arn"))])
    role("IntakeRole", "lambda.amazonaws.com", [
        log_allow("IntakeLogs"), allow(["sqs:SendMessage"], get("IngressQueue", "Arn")),
        allow(["sqs:GetQueueAttributes"], [get("IngressQueue", "Arn"), get("Dlq", "Arn")]),
        allow(["cloudwatch:GetMetricStatistics"], "*"),
        allow(["lambda:InvokeFunction"], sub(
            "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${AWS::StackName}-writer-control")),
        allow(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
              sub("${DataBucket.Arn}/spaces/*/ingest/*")),
        allow(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
              sub("${DataBucket.Arn}/spaces/*/ingestion-replay/*")),
        allow(["dynamodb:GetItem"], get("ControlTable", "Arn"),
              {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["SPACE#*", "KEYHASH#*", "WRITER#PRIMARY"]}}),
        # Replay intents enter the space audit trail; intake writes no other control record.
        allow(["dynamodb:PutItem"], get("ControlTable", "Arn"),
              {"ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["AUDIT#*", "AUDITMARK#*"]}})])
    role("SyncRole", "lambda.amazonaws.com", [
        log_allow("SyncLogs"),
        allow(["dynamodb:GetItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["SPACE#*", "KEYHASH#*", "WRITER#PRIMARY"]}}),
        allow(["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface",
               "ec2:AssignPrivateIpAddresses", "ec2:UnassignPrivateIpAddresses"], "*")])
    role("ExecutionRole", "ecs-tasks.amazonaws.com", [
        allow(["ecr:GetAuthorizationToken"], "*"),
        allow(["ecr:BatchCheckLayerAvailability", "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage"],
              [sub("arn:${AWS::Partition}:ecr:${AWS::Region}:${AWS::AccountId}:repository/colvenna-isle-images"),
               "arn:aws:ecr:${AWS::Region}:*:repository/aws-guardduty-agent-fargate"]),
        log_allow("TaskLogs")])
    role("WriterRole", "ecs-tasks.amazonaws.com", [
        allow(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObject"],
              sub("${DataBucket.Arn}/spaces/*/gen_*/warehouse/*")),
        allow(["s3:GetObject", "s3:GetObjectVersion"], sub("${DataBucket.Arn}/spaces/*/ingest/*")),
        allow(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
              sub("${DataBucket.Arn}/spaces/*/write-intents/gen_*/*")),
        allow(["s3:GetObjectVersion"], sub("${DataBucket.Arn}/spaces/*/query-intents/gen_*/*")),
        allow(["s3:GetObjectVersion"], sub("${DataBucket.Arn}/spaces/*/qualification-import/gen_*/*")),
        allow(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject"],
              sub("${DataBucket.Arn}/spaces/*/inputs/gen_*/*")),
        allow(["s3:GetObjectVersion"], [
            sub("${DataBucket.Arn}/spaces/*/task-contexts/gen_*/*"),
            sub("${DataBucket.Arn}/spaces/*/staging/gen_*/*"),
            sub("${DataBucket.Arn}/spaces/*/pipeline-code/gen_*/*")]),
        # Writer-executor SQL stages the same immutable task contract as Lambda/ECS.
        allow(["s3:PutObject"], sub("${DataBucket.Arn}/spaces/*/staging/gen_*/*")),
        allow(["s3:ListBucketVersions"], get("DataBucket", "Arn"),
              {"StringLike": {"s3:prefix": ["spaces/*/inputs/gen_*/*"]}}),
        allow(["s3:ListBucket"], get("DataBucket", "Arn"),
              {"StringLike": {"s3:prefix": ["spaces/*/gen_*/warehouse", "spaces/*/gen_*/warehouse/*"]}}),
        # Native snapshots are written once and restores read exact versions; no delete right.
        {"Fn::If": ["HasBackupBucket", allow(["s3:PutObject", "s3:GetObject", "s3:GetObjectVersion"],
                                             sub("arn:aws:s3:::${BackupBucket}/backups/*")), ref("AWS::NoValue")]},
        allow(["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:ChangeMessageVisibility", "sqs:GetQueueAttributes"],
              get("IngressQueue", "Arn")),
        allow(["sqs:ReceiveMessage", "sqs:DeleteMessage"], get("Dlq", "Arn")),
        allow(["dynamodb:GetItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": [
                "SPACE#*", "KEYID#*", "KEYHASH#*", "WRITER#PRIMARY", "WRITERPROCESS#*", "ENVIRONMENT#1#*",
                "QUERY#*"]}}),
        allow(["dynamodb:PutItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["QUERY#*"]}}),
        allow(["dynamodb:PutItem", "dynamodb:UpdateItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["WRITERPROCESS#*"]}}),
        allow(["dynamodb:PutItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["SPACE#*"]},
            "ForAllValues:StringEquals": {"dynamodb:Attributes": [
                "PK", "SK", "generation", "projection_version", "observation_sha256", "observation"]}})])
    role("WriterControlRole", "lambda.amazonaws.com", [
        log_allow("TaskLogs"),
        allow(["dynamodb:GetItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["WRITER#PRIMARY", "WRITERPROCESS#*"]}}),
        allow(["ecs:DescribeTasks"], sub("arn:${AWS::Partition}:ecs:${AWS::Region}:${AWS::AccountId}:task/${Cluster}/*")),
        allow(["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface",
               "ec2:AssignPrivateIpAddresses", "ec2:UnassignPrivateIpAddresses"], "*")])
    # Qualified READ_ONLY readers create managed checkpoints in manifest only.
    role("QueryRole", "ecs-tasks.amazonaws.com", [
        allow(["dynamodb:GetItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": [
                "SPACE#*", "KEYHASH#*", "KEYID#*", "QUERY#*", "WRITER#PRIMARY"]}}),
        allow(["lambda:InvokeFunction"],
              sub("arn:${AWS::Partition}:lambda:${AWS::Region}:${AWS::AccountId}:function:${AWS::StackName}-writer-control")),
        allow(["dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:Query"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["QUERY#*"]}}),
        allow(["dynamodb:ConditionCheckItem"], get("ControlTable", "Arn"), {
            "ForAllValues:StringLike": {"dynamodb:LeadingKeys": ["SPACE#*"]}}),
        allow(["ecs:DescribeTasks"], sub("arn:${AWS::Partition}:ecs:${AWS::Region}:${AWS::AccountId}:task/${Cluster}/*")),
        allow(["s3:ListBucket"], get("DataBucket", "Arn"), {
            "StringLike": {"s3:prefix": ["spaces/*/gen_*/warehouse/duckdb-object-storage/warehouse_*.db/*"]}}),
        allow(["s3:ListBucketVersions"], get("DataBucket", "Arn"), {
            "StringLike": {"s3:prefix": [
                "spaces/*/query-intents/gen_*/*", "spaces/*/query-results/gen_*/*"]}}),
        allow(["s3:GetObject", "s3:GetObjectVersion"], sub("${DataBucket.Arn}/spaces/*/gen_*/warehouse/duckdb-object-storage/warehouse_*.db/*")),
        allow(["s3:PutObject"], sub("${DataBucket.Arn}/spaces/*/gen_*/warehouse/duckdb-object-storage/warehouse_*.db/manifest/*")),
        allow(["s3:GetObject", "s3:GetObjectVersion", "s3:PutObject", "s3:DeleteObjectVersion"], [
            sub("${DataBucket.Arn}/spaces/*/query-intents/gen_*/*"),
            sub("${DataBucket.Arn}/spaces/*/query-results/gen_*/*")])])
    add("Authorizer", "AWS::Lambda::Function", Runtime="python3.12", Architectures=["arm64"],
        Handler={"Fn::If": ["HasAuthCode", "colvenna_isle.control.authorizer.handler", "index.handler"]},
        Role=get("AuthorizerRole", "Arn"), Timeout=10, MemorySize=256,
        LoggingConfig={"LogGroup": ref("AuthorizerLogs")},
        Environment={"Variables": {"COLVENNA_CONTROL_TABLE": ref("ControlTable"),
                                   "COLVENNA_AUTH_ENABLED": ref("AuthEnabled"),
                                   "COLVENNA_SOURCE_COMMIT": {"Fn::If": [
                                       "HasAuthCode", ref("AuthSourceCommit"), ref("SourceCommit")]}}},
        Code={"Fn::If": ["HasAuthCode", {"S3Bucket": ref("AuthCodeBucket"), "S3Key": ref("AuthCodeKey"),
                                        "S3ObjectVersion": ref("AuthCodeVersion")},
                         {"ZipFile": "def handler(event, context):\n    return {'isAuthorized': False}\n"}]}, Tags=tags)
    intake_environment = {"Variables": {
        "COLVENNA_CONTROL_TABLE": ref("ControlTable"), "COLVENNA_DATA_BUCKET": ref("DataBucket"),
        "COLVENNA_INGRESS_QUEUE": ref("IngressQueue"), "COLVENNA_ACCOUNT": ref("AWS::AccountId"),
        "COLVENNA_DLQ": ref("Dlq"),
        "COLVENNA_WRITER_CONTROL_FUNCTION": sub("${AWS::StackName}-writer-control"),
        "COLVENNA_INTAKE_ENABLED": ref("IntakeEnabled"), "COLVENNA_TIMING": ref("Timing"),
        "COLVENNA_SOURCE_COMMIT": {"Fn::If": ["HasIntakeCode", ref("IntakeSourceCommit"), ref("SourceCommit")]},
    }}
    intake_code = {"S3Bucket": ref("IntakeCodeBucket"), "S3Key": ref("IntakeCodeKey"),
                   "S3ObjectVersion": ref("IntakeCodeVersion")}
    add("Intake", "AWS::Lambda::Function", Runtime="python3.12", Architectures=["arm64"],
        Handler={"Fn::If": ["HasIntakeCode", "colvenna_isle.intake.handler.handler", "index.handler"]},
        Role=get("IntakeRole", "Arn"), Timeout=60, MemorySize=256,
        LoggingConfig={"LogGroup": ref("IntakeLogs")}, Environment=intake_environment,
        Code={"Fn::If": ["HasIntakeCode", intake_code, {
            "ZipFile": "def handler(event, context):\n    return {'statusCode': 403, 'body': 'Unavailable'}\n"}]}, Tags=tags)
    resources["IntakeProbe"] = {"Type": "AWS::Lambda::Function", "Condition": "HasIntakeCode", "Properties": {
        "Runtime": "python3.12", "Architectures": ["arm64"],
        "Handler": "colvenna_isle.intake.handler.probe_handler", "Role": get("IntakeRole", "Arn"),
        "Timeout": 60, "MemorySize": 256, "LoggingConfig": {"LogGroup": ref("IntakeLogs")},
        "Environment": intake_environment, "Code": intake_code, "Tags": tags,
    }}
    add("Sync", "AWS::Lambda::Function", Runtime="python3.12", Architectures=["arm64"],
        FunctionName=sub("${AWS::StackName}-sync"),
        Handler={"Fn::If": ["HasSyncCode", "colvenna_isle.sync.handler.handler", "index.handler"]},
        Role=get("SyncRole", "Arn"), Timeout=28, MemorySize=256,
        LoggingConfig={"LogGroup": ref("SyncLogs")},
        VpcConfig={"SubnetIds": [ref("PrivateSubnet1"), ref("PrivateSubnet2")],
                   "SecurityGroupIds": [ref("SyncSecurityGroup")]},
        Environment={"Variables": {
            "COLVENNA_CONTROL_TABLE": ref("ControlTable"), "COLVENNA_DATA_BUCKET": ref("DataBucket"),
            "COLVENNA_SYNC_ENABLED": ref("SyncEnabled"), "COLVENNA_TIMING": ref("Timing"),
            "COLVENNA_SOURCE_COMMIT": {"Fn::If": ["HasSyncCode", ref("SyncSourceCommit"), ref("SourceCommit")]}}},
        Code={"Fn::If": ["HasSyncCode", {
            "S3Bucket": ref("SyncCodeBucket"), "S3Key": ref("SyncCodeKey"),
            "S3ObjectVersion": ref("SyncCodeVersion")}, {
            "ZipFile": "def handler(event, context):\n    return {'statusCode': 503, 'body': 'Unavailable'}\n"}]},
        Tags=tags)
    resources["WriterControl"] = {"Type": "AWS::Lambda::Function", "Condition": "HasWriterCode", "Properties": {
        "FunctionName": sub("${AWS::StackName}-writer-control"),
        "Runtime": "python3.12", "Architectures": ["arm64"], "Handler": "colvenna_isle.writer.bridge.handler",
        "Role": get("WriterControlRole", "Arn"), "Timeout": 30, "MemorySize": 256,
        "LoggingConfig": {"LogGroup": ref("TaskLogs")},
        "VpcConfig": {"SubnetIds": [ref("PrivateSubnet1"), ref("PrivateSubnet2")],
                      "SecurityGroupIds": [ref("WriterControlSecurityGroup")]},
        "Environment": {"Variables": {
            "COLVENNA_CONTROL_TABLE": ref("ControlTable"), "COLVENNA_CLUSTER": ref("Cluster"),
            "COLVENNA_WRITER_PROBE_SPACE": ref("WriterProbeSpace"),
            "COLVENNA_SOURCE_COMMIT": ref("WriterSourceCommit")}},
        "Code": {"S3Bucket": ref("WriterCodeBucket"), "S3Key": ref("WriterCodeKey"),
                 "S3ObjectVersion": ref("WriterCodeVersion")}, "Tags": tags,
    }}
    add("Cluster", "AWS::ECS::Cluster", ClusterSettings=[{"Name": "containerInsights", "Value": "enabled"}],
        Tags=tags)
    health = (
        "from http.server import BaseHTTPRequestHandler,HTTPServer\n"
        "class H(BaseHTTPRequestHandler):\n"
        " def log_message(self,*args): pass\n"
        " def do_GET(self):\n"
        "  self.send_response(200 if self.path=='/health' else 403);self.end_headers();self.wfile.write(b'baseline')\n"
        "HTTPServer(('0.0.0.0',8080),H).serve_forever()\n"
    )
    for name, task_role, code in [
        ("WriterTask", "WriterRole", "import time;print('baseline placeholder: business writer NOT_IMPLEMENTED',flush=True);time.sleep(86400)"),
        ("QueryTask", "QueryRole", health),
    ]:
        add(name, "AWS::ECS::TaskDefinition", Cpu="2048", Memory="4096", NetworkMode="awsvpc",
            RequiresCompatibilities=["FARGATE"], RuntimePlatform={
                "CpuArchitecture": "ARM64", "OperatingSystemFamily": "LINUX"},
            EphemeralStorage={"SizeInGiB": 21}, ExecutionRoleArn=get("ExecutionRole", "Arn"),
            TaskRoleArn=get(task_role, "Arn"), ContainerDefinitions=[{
                "Name": "baseline", "Image": ref("StorageImage"), "Essential": True,
                "EntryPoint": ["python", "-c"], "Command": [code], "ReadonlyRootFilesystem": True,
                "User": "10001:10001",
                "Environment": [{"Name": "COLVENNA_SOURCE_COMMIT", "Value": ref("SourceCommit")}],
                "PortMappings": [{"ContainerPort": 8080}] if name == "QueryTask" else [],
                "LogConfiguration": {"LogDriver": "awslogs", "Options": {
                    "awslogs-group": ref("TaskLogs"), "awslogs-region": "${AWS::Region}",
                    "awslogs-stream-prefix": name}},
            }], Tags=tags)
    writer = resources["WriterTask"]["Properties"]
    container = writer["ContainerDefinitions"][0]
    container.update(
        Image={"Fn::If": ["HasWriterCode", ref("WriterImage"), ref("StorageImage")]},
        EntryPoint={"Fn::If": ["HasWriterCode",
                             ["python", "-m", "colvenna_isle.writer.runtime"], ["python", "-c"]]},
        Command={"Fn::If": ["HasWriterCode", [], [
            "import time;print('baseline placeholder: business writer NOT_IMPLEMENTED',flush=True);time.sleep(86400)"]]},
        Environment=[{"Name": key, "Value": value} for key, value in {
            "COLVENNA_SOURCE_COMMIT": {"Fn::If": ["HasWriterCode", ref("WriterSourceCommit"), ref("SourceCommit")]},
            "COLVENNA_CONTROL_TABLE": ref("ControlTable"), "COLVENNA_DATA_BUCKET": ref("DataBucket"),
            "COLVENNA_INGRESS_QUEUE": ref("IngressQueue"), "COLVENNA_DLQ": ref("Dlq"), "COLVENNA_REGION": ref("AWS::Region"), "AWS_REGION": ref("AWS::Region"), "AWS_DEFAULT_REGION": ref("AWS::Region"),
            "COLVENNA_ACCOUNT": ref("AWS::AccountId"), "COLVENNA_INTAKE_SOURCE_COMMIT": ref("IntakeSourceCommit"),
            "COLVENNA_ENVIRONMENT_POLICY": ref("WriterEnvironmentPolicy"),
            "COLVENNA_QUERY_SOURCE_COMMIT": ref("QuerySourceCommit"), "COLVENNA_QUERY_IMAGE": ref("QueryImage"),
            "COLVENNA_WRITER_PROBE_SPACE": ref("WriterProbeSpace"),
            "COLVENNA_WRITER_PROBE_GENERATION": ref("WriterProbeGeneration"), "COLVENNA_TIMING": ref("Timing"),
            "COLVENNA_BACKUP_BUCKET": ref("BackupBucket"),
        }.items()],
        PortMappings=[{"ContainerPort": 8081}, {"ContainerPort": 8082}],
        MountPoints=[{"SourceVolume": "writer-work", "ContainerPath": "/work", "ReadOnly": False}],
        StopTimeout=30)
    writer["Volumes"] = [{"Name": "writer-work"}]
    query = resources["QueryTask"]["Properties"]
    query["ContainerDefinitions"][0].update(
        Image={"Fn::If": ["HasQueryCode", ref("QueryImage"), ref("StorageImage")]},
        EntryPoint={"Fn::If": ["HasQueryCode",
                             ["python", "-m", "colvenna_isle.query.runtime"], ["python", "-c"]]},
        Command={"Fn::If": ["HasQueryCode", [], [health]]},
        Environment=[{"Name": key, "Value": value} for key, value in {
            "COLVENNA_SOURCE_COMMIT": {"Fn::If": ["HasQueryCode", ref("QuerySourceCommit"), ref("SourceCommit")]},
            "COLVENNA_CONTROL_TABLE": ref("ControlTable"), "COLVENNA_DATA_BUCKET": ref("DataBucket"),
            "COLVENNA_REGION": ref("AWS::Region"), "AWS_REGION": ref("AWS::Region"), "AWS_DEFAULT_REGION": ref("AWS::Region"), "COLVENNA_ACCOUNT": ref("AWS::AccountId"),
            "COLVENNA_QUERY_API_URL": get("Api", "ApiEndpoint"),
            "COLVENNA_QUERY_IMAGE": ref("QueryImage"), "COLVENNA_WRITER_SOURCE_COMMIT": ref("WriterSourceCommit"),
            "COLVENNA_WRITER_IMAGE": ref("WriterImage"),
            "COLVENNA_WRITER_FUNCTION": sub("${AWS::StackName}-writer-control"),
            "COLVENNA_ECS_CLUSTER": ref("Cluster"), "COLVENNA_TIMING": ref("Timing"),
        }.items()],
        MountPoints=[{"SourceVolume": "query-work", "ContainerPath": "/work", "ReadOnly": False}],
        StopTimeout=30)
    query["Volumes"] = [{"Name": "query-work"}]
    add("Alb", "AWS::ElasticLoadBalancingV2::LoadBalancer", Scheme="internal", Type="application",
        Subnets=[ref("PrivateSubnet1"), ref("PrivateSubnet2")], SecurityGroups=[ref("AlbSecurityGroup")],
        LoadBalancerAttributes=[{"Key": "routing.http.drop_invalid_header_fields.enabled", "Value": "true"},
                                {"Key": "routing.http.desync_mitigation_mode", "Value": "defensive"}], Tags=tags)
    add("QueryTargets", "AWS::ElasticLoadBalancingV2::TargetGroup", Port=8080, Protocol="HTTP",
        TargetType="ip", VpcId=ref("Vpc"), HealthCheckPath="/health", HealthCheckIntervalSeconds=10,
        HealthyThresholdCount=2, TargetGroupAttributes=[{"Key": "deregistration_delay.timeout_seconds", "Value": "10"}],
        Tags=tags)
    add("Listener", "AWS::ElasticLoadBalancingV2::Listener", LoadBalancerArn=ref("Alb"), Port=80,
        Protocol="HTTP", DefaultActions=[{"Type": "forward", "TargetGroupArn": ref("QueryTargets")}])
    for name, task, group, desired in [
        ("WriterService", "WriterTask", "WriterSecurityGroup", 0),
        ("QueryService", "QueryTask", "QuerySecurityGroup", ref("QueryDesired")),
    ]:
        properties = {
            "Cluster": ref("Cluster"), "TaskDefinition": ref(task), "DesiredCount": desired,
            "LaunchType": "FARGATE", "PlatformVersion": "1.4.0",
            "DeploymentConfiguration": {"MinimumHealthyPercent": 0, "MaximumPercent": 100,
                                        "DeploymentCircuitBreaker": {"Enable": True, "Rollback": True}},
            "NetworkConfiguration": {"AwsvpcConfiguration": {
                "AssignPublicIp": "DISABLED", "Subnets": [ref("PrivateSubnet1"), ref("PrivateSubnet2")],
                "SecurityGroups": [ref(group)]}}, "EnableExecuteCommand": False, "Tags": tags,
        }
        if name == "QueryService":
            properties["LoadBalancers"] = [{"ContainerName": "baseline", "ContainerPort": 8080,
                                           "TargetGroupArn": ref("QueryTargets")}]
            properties["HealthCheckGracePeriodSeconds"] = 30
        add(name, "AWS::ECS::Service", **properties)
        resources[name]["DependsOn"] = ["NatRoute", "Listener"]
    add("VpcLink", "AWS::ApiGatewayV2::VpcLink", Name=sub("${AWS::StackName}-query"),
        SubnetIds=[ref("PrivateSubnet1"), ref("PrivateSubnet2")], SecurityGroupIds=[ref("LinkSecurityGroup")])
    add("Api", "AWS::ApiGatewayV2::Api", Name=sub("${AWS::StackName}-api"), ProtocolType="HTTP",
        DisableExecuteApiEndpoint=False)
    add("ApiAuthorizer", "AWS::ApiGatewayV2::Authorizer", ApiId=ref("Api"), Name="ColvennaIsleBearer",
        AuthorizerType="REQUEST", AuthorizerPayloadFormatVersion="2.0", EnableSimpleResponses=True,
        IdentitySource=["$request.header.Authorization"], AuthorizerResultTtlInSeconds=0,
        AuthorizerUri=sub("arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${Authorizer.Arn}/invocations"))
    for name, function, suffix in [
        ("AuthorizerPermission", "Authorizer", "authorizers/*"),
        ("IntakePermission", "Intake", "*/*"),
        ("SyncPermission", "Sync", "*/*/v1/spaces/*/writes*"),
    ]:
        add(name, "AWS::Lambda::Permission", FunctionName=ref(function), Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com", SourceAccount=ref("AWS::AccountId"),
            SourceArn=sub("arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${Api}/"+suffix))
    add("IntakeIntegration", "AWS::ApiGatewayV2::Integration", ApiId=ref("Api"),
        IntegrationType="AWS_PROXY", IntegrationUri=get("Intake", "Arn"), PayloadFormatVersion="2.0")
    add("SyncIntegration", "AWS::ApiGatewayV2::Integration", ApiId=ref("Api"),
        IntegrationType="AWS_PROXY", IntegrationUri=get("Sync", "Arn"),
        PayloadFormatVersion="2.0", TimeoutInMillis=30000)
    add("QueryIntegration", "AWS::ApiGatewayV2::Integration", ApiId=ref("Api"),
        IntegrationType="HTTP_PROXY", IntegrationMethod="ANY", IntegrationUri=ref("Listener"),
        ConnectionType="VPC_LINK", ConnectionId=ref("VpcLink"), PayloadFormatVersion="1.0",
        RequestParameters={"overwrite:path": "$request.path", **{
            "overwrite:header." + header: "$context.authorizer." + name for name, header in {
                "principal_id": "x-colvenna-principal-id", "key_id": "x-colvenna-key-id",
                "spaces": "x-colvenna-spaces", "scopes": "x-colvenna-scopes",
                "generation": "x-colvenna-generation", "expires_at": "x-colvenna-expires-at",
            }.items()}})
    for name, route, integration in [
        ("DefaultRoute", "$default", "IntakeIntegration"),
        ("CatalogRoute", "GET /v1/spaces/{space_id}/catalog", "QueryIntegration"),
        ("QueryRoute", "POST /v1/spaces/{space_id}/queries", "QueryIntegration"),
        ("QueryStatusRoute", "GET /v1/spaces/{space_id}/queries/{query_id}", "QueryIntegration"),
        ("QueryHistoryRoute", "GET /v1/spaces/{space_id}/queries", "QueryIntegration"),
        ("QueryExportRoute", "GET /v1/spaces/{space_id}/queries/{query_id}/export", "QueryIntegration"),
        ("QueryCancelRoute", "POST /v1/spaces/{space_id}/queries/{query_id}/cancel", "QueryIntegration"),
        ("SyncWriteRoute", "POST /v1/spaces/{space_id}/writes", "SyncIntegration"),
        ("SyncStatusRoute", "GET /v1/spaces/{space_id}/writes/{write_id}", "SyncIntegration"),
    ]:
        add(name, "AWS::ApiGatewayV2::Route", ApiId=ref("Api"), RouteKey=route,
            AuthorizationType="CUSTOM", AuthorizerId=ref("ApiAuthorizer"),
            Target=sub("integrations/${"+integration+"}"))
    add("Stage", "AWS::ApiGatewayV2::Stage", ApiId=ref("Api"), StageName="$default", AutoDeploy=True,
        DefaultRouteSettings={"ThrottlingBurstLimit": 20, "ThrottlingRateLimit": 10},
        AccessLogSettings={"DestinationArn": get("ApiLogs", "Arn"),
                           "Format": '{"requestId":"$context.requestId","status":"$context.status","route":"$context.routeKey"}'})
    # CloudFront names are account-global, even though their stacks are regional.
    add("OriginAccess", "AWS::CloudFront::OriginAccessControl",
        OriginAccessControlConfig={"Name": sub("${AWS::StackName}-${AWS::Region}-web"), "OriginAccessControlOriginType": "s3",
                                   "SigningBehavior": "always", "SigningProtocol": "sigv4"})
    add("Headers", "AWS::CloudFront::ResponseHeadersPolicy", ResponseHeadersPolicyConfig={
        "Name": sub("${AWS::StackName}-${AWS::Region}-security"), "SecurityHeadersConfig": {
            "ContentSecurityPolicy": {"ContentSecurityPolicy":
                "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
                "base-uri 'none'; frame-ancestors 'none'; form-action 'none'", "Override": True},
            "StrictTransportSecurity": {"AccessControlMaxAgeSec": 31536000, "IncludeSubdomains": True, "Override": True},
            "ContentTypeOptions": {"Override": True}, "FrameOptions": {"FrameOption": "DENY", "Override": True},
            "ReferrerPolicy": {"ReferrerPolicy": "no-referrer", "Override": True}}})
    add("BrowserApiCache", "AWS::CloudFront::CachePolicy", CachePolicyConfig={
        "Name": sub("${AWS::StackName}-${AWS::Region}-browser-api-no-cache"),
        "DefaultTTL": 0, "MinTTL": 0, "MaxTTL": 0,
        "ParametersInCacheKeyAndForwardedToOrigin": {
            "EnableAcceptEncodingGzip": False, "EnableAcceptEncodingBrotli": False,
            "HeadersConfig": {"HeaderBehavior": "none"}, "CookiesConfig": {"CookieBehavior": "none"},
            "QueryStringsConfig": {"QueryStringBehavior": "none"}}})
    add("BrowserApiForwarding", "AWS::CloudFront::OriginRequestPolicy", OriginRequestPolicyConfig={
        "Name": sub("${AWS::StackName}-${AWS::Region}-browser-api"),
        # Bearer authorization reaches Gateway; the origin supplies its own Host.
        "HeadersConfig": {"HeaderBehavior": "allExcept", "Headers": ["host"]},
        "CookiesConfig": {"CookieBehavior": "none"},
        "QueryStringsConfig": {"QueryStringBehavior": "all"}})
    add("Distribution", "AWS::CloudFront::Distribution", DistributionConfig={
        "Enabled": True, "DefaultRootObject": "index.html", "HttpVersion": "http2",
        "PriceClass": "PriceClass_100", "ViewerCertificate": {"CloudFrontDefaultCertificate": True},
        "Origins": [
            {"Id": "private-web", "DomainName": get("WebBucket", "RegionalDomainName"),
             "S3OriginConfig": {"OriginAccessIdentity": ""}, "OriginAccessControlId": ref("OriginAccess")},
            {"Id": "browser-api", "DomainName": sub("${Api}.execute-api.${AWS::Region}.${AWS::URLSuffix}"),
             "CustomOriginConfig": {"HTTPSPort": 443, "OriginProtocolPolicy": "https-only",
                                   "OriginSSLProtocols": ["TLSv1.2"]}}],
        "DefaultCacheBehavior": {"TargetOriginId": "private-web", "ViewerProtocolPolicy": "redirect-to-https",
                                 "AllowedMethods": ["GET", "HEAD"], "CachedMethods": ["GET", "HEAD"],
                                 "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",
                                 "ResponseHeadersPolicyId": ref("Headers")},
        "CacheBehaviors": [{
            "PathPattern": "v1/*", "TargetOriginId": "browser-api", "ViewerProtocolPolicy": "https-only",
            "AllowedMethods": ["GET", "HEAD", "OPTIONS", "PUT", "PATCH", "POST", "DELETE"],
            "CachedMethods": ["GET", "HEAD"], "CachePolicyId": ref("BrowserApiCache"),
            "OriginRequestPolicyId": ref("BrowserApiForwarding"), "ResponseHeadersPolicyId": ref("Headers"),
        }],
        "CustomErrorResponses": [
            {"ErrorCode": code, "ErrorCachingMinTTL": 0}
            for code in [400, 403, 404, 405, 414, 500, 501, 502, 503, 504]],
        }, Tags=tags)
    add("WebPolicy", "AWS::S3::BucketPolicy", Bucket=ref("WebBucket"), PolicyDocument={
        "Version": "2012-10-17", "Statement": [tls_statement("WebBucket"), {
            "Effect": "Allow", "Principal": {"Service": "cloudfront.amazonaws.com"}, "Action": "s3:GetObject",
            "Resource": [sub("${WebBucket.Arn}/" + name) for name in WEB_FILES],
            "Condition": {"StringEquals": {"AWS:SourceArn":
                          sub("arn:${AWS::Partition}:cloudfront::${AWS::AccountId}:distribution/${Distribution}")}}}]})
    outputs = {
        name: {"Value": ref(name)} for name in ["DataBucket", "WebBucket", "ControlTable", "IngressQueue",
               "Dlq", "Cluster", "WriterTask", "QueryTask", "WriterService", "QueryService", "TaskLogs",
               "PrivateSubnet1", "PrivateSubnet2", "WriterSecurityGroup", "QuerySecurityGroup",
               "LinkSecurityGroup", "AlbSecurityGroup", "VpcLink", "Api", "Vpc", "Distribution"]
    }
    outputs.update({
        "ApiUrl": {"Value": get("Api", "ApiEndpoint")},
        "WebUrl": {"Value": sub("https://${Distribution.DomainName}")},
        "AlbArn": {"Value": ref("Alb")}, "AlbDns": {"Value": get("Alb", "DNSName")},
        "QueryTargetArn": {"Value": ref("QueryTargets")},
        "WriterRoleArn": {"Value": get("WriterRole", "Arn")},
        "QueryRoleArn": {"Value": get("QueryRole", "Arn")},
        "QueryImage": {"Value": ref("QueryImage")},
        "QuerySourceCommit": {"Value": ref("QuerySourceCommit")},
        "BrowserSourceCommit": {"Value": ref("BrowserSourceCommit")},
        "IntakeRoleArn": {"Value": get("IntakeRole", "Arn")},
        "ExecutionRoleArn": {"Value": get("ExecutionRole", "Arn")},
        "AuthorizerFunction": {"Value": ref("Authorizer")},
        "AuthorizerLogs": {"Value": ref("AuthorizerLogs")},
        "IntakeFunction": {"Value": ref("Intake")},
        "IntakeProbeFunction": {"Condition": "HasIntakeCode", "Value": ref("IntakeProbe")},
        "IntakeLogs": {"Value": ref("IntakeLogs")},
        "SyncFunction": {"Value": ref("Sync")},
        "SyncLogs": {"Value": ref("SyncLogs")},
        "SyncRoleArn": {"Value": get("SyncRole", "Arn")},
        "SyncSecurityGroup": {"Value": ref("SyncSecurityGroup")},
        "SyncSourceCommit": {"Value": ref("SyncSourceCommit")},
        "SourceCommit": {"Value": ref("SourceCommit")}, "StorageImage": {"Value": ref("StorageImage")},
        "WriterImage": {"Value": ref("WriterImage")},
        "WriterSourceCommit": {"Value": ref("WriterSourceCommit")},
        "WriterControlFunction": {"Condition": "HasWriterCode", "Value": ref("WriterControl")},
        "BackupBucket": {"Value": ref("BackupBucket")},
    })
    for resource in resources.values():
        if resource["Type"] == "AWS::Lambda::Function":
            resource["Properties"].setdefault("Environment", {}).setdefault("Variables", {})["COLVENNA_REGION"] = ref("AWS::Region")
    return regional({"AWSTemplateFormatVersion": "2010-09-09", "Description": "Colvenna Isle private AWS baseline",
            "Parameters": parameters,
            "Conditions": {
                "HasAuthCode": {"Fn::Not": [{"Fn::Equals": [ref("AuthCodeKey"), ""]}]},
                "HasIntakeCode": {"Fn::Not": [{"Fn::Equals": [ref("IntakeCodeKey"), ""]}]},
                "HasSyncCode": {"Fn::Not": [{"Fn::Equals": [ref("SyncCodeKey"), ""]}]},
                "HasWriterCode": {"Fn::Not": [{"Fn::Equals": [ref("WriterCodeKey"), ""]}]},
                "HasQueryCode": {"Fn::Not": [{"Fn::Equals": [ref("QueryImage"), ""]}]},
                "HasBackupBucket": {"Fn::Not": [{"Fn::Equals": [ref("BackupBucket"), ""]}]},
            },
            "Resources": resources, "Outputs": outputs})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(json.dumps(template(), separators=(",", ":")) + "\n")
