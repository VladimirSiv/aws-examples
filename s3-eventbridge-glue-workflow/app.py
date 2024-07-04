from aws_cdk import App
from s3_eventbridge_glue_workflow import S3EventBridgeGlueWorkflowStack

app = App()
S3EventBridgeGlueWorkflowStack(app, "s3-eventbridge-glue-workflow")

app.synth()
