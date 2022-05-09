# Cloud Development Kit ETL Pipeline
{: .fs-9 .no_toc}

An extensible CDK ETL pipeline template written in TypeScript to help speed up the data engineering transformation process.
{: .fs-6 .fw-300 }

[Get started now](#project){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 } [View it on GitHub](https://github.com/venGaza/etlPipeline){: .btn .fs-5 .mb-4 .mb-md-0 }

---

<details open markdown="block">
  <summary>
    Table of contents
  </summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

## Project
Data engineering is the most time consuming process of any data analytics or machine learning project which invovles the collection, storage, processing, analysis, and visualization of data. Raw data is typically of no use to businesses until it has been transformed/cleaned.  This project aims to provide a template to help data engineers and data scientists provision a robust ETL pipeline through the AWS CDK using TypeScript and AWS Glue. 

### AWS ETL Architecture  
<p align="center"><img src="assets/architecture.jpg"></p>

## Features

* Optimize raw data for analytics by automatically transforming CSV, JSON, and XML documents into a compressed Parquet format.
* This template is built to be modular.  Need another transformation or another stage in the pipeline? Just add another Glue job into the workflow or create another workflow from the existing template. 
* Remove PII (Coming soon)
* Partition the documents by datetime column (Coming soon)


## Installation

### AWS CLI
1. Clone the project to the local file directory using the AWS Cloudshell
```bash
$ git clone https://github.com/venGaza/etlPipeline 
```
** Note this project can be downloaded to local computer but make sure to have the following dependencies installed: AWS CLI, Node, CDK(NPM Package)

2. Provision the resources required by CDK (S3 bucket, IAM roles, etc.)
```bash
$ cdk bootstrap
```

3. Move into the file directory and deploy the CDK application
```bash
$ cdk deploy
```

4. There should be an output indicating in the CLI indicating a successful deployment of the stack. Verify the new stack exists:
```bash
$ aws cloudformation list-stacks
```

5. (Optional )Navigate to the CloudFormation Console in the AWS Console. The etlPipeline stack should be viewable. 

## Uninstall
### AWS Console
1. Navigate to the CloudFormation Console in the AWS Console.
2. Select the name of the stack.
3. Press the delete button located at the top of the list of stacks.

### AWS CloudShell
1. Run the following command in the AWS CLI from within the application directory
```bash
$ cdk destroy
```

2. There should be an output confirming the successful deletion of the stack. Verify the stack no longer exists:
```bash
$ aws cloudformation list-stacks 
```

## Useful commands
* `cdk deploy`      deploy this stack to your default AWS account/region
* `cdk destroy`     destroy this stack
* `cdk bootstrap`   provision resources for cdk
* `aws cloudformation list-stacks`        compare deployed stack with current state

## About

etlPipeline is &copy; 2022

### Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change.

#### Thank you to the other contributors to this project
- Tom Anson
- Sahil Patel
- Reuben Mackintosh