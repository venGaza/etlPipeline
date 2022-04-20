import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3Deploy from 'aws-cdk-lib/aws-s3-deployment'
import * as glue from 'aws-cdk-lib/aws-glue';
import { aws_athena as athena } from 'aws-cdk-lib';
import { Construct } from 'constructs';
//import * as iam from 'aws-cdk-lib/aws-iam'
//import { Role } from 'aws-cdk-lib/aws-iam';

// TODO
// Bucket Encryption
// Glue Trigger Event or Schedule Crawler Hourly


export class PipelineStack extends cdk.Stack {

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 Object Keys
    const landingZonePath = "landing-zone-444";
    const cleanZonePath = "clean-zone-444";

    // Provision bucket for raw data
    const landing_zone_bucket = new  s3.Bucket(this, landingZonePath, {
        versioned: true,
        publicReadAccess: false,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Provision bucket for clean data
    const clean_zone_bucket =new s3.Bucket(this, cleanZonePath, {
      versioned: true,
      publicReadAccess: false,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    })

    // Upload test data into landing zone bucket
    // (Make sure to put files in OWN folder or tables WILL NOT work)
    new s3Deploy.BucketDeployment(this, "DeployTestData", {
      sources: [s3Deploy.Source.asset("./data")], 
      destinationBucket: landing_zone_bucket,
      destinationKeyPrefix: "",
      retainOnDelete: false,
      exclude: ['.*'],
    });

    // Create Glue database
    const glue_db = new glue.CfnDatabase(this, "pipeline-db", {
      catalogId: "911320291896",
      databaseInput: {
        name: "etl_pipeline",
        description: "Store tables for raw data",
      }
    })

    // Define paths for data
    const raw_data = "s3://" + landing_zone_bucket.bucketName + '/'
    const clean_data = "s3://" + clean_zone_bucket.bucketName + '/'

    // Create tables in Glue DB for CSV data in landing zone
    const landingZoneCrawler = new glue.CfnCrawler(this, "landingZoneCrawler", {
      role: 'arn:aws:iam::911320291896:role/AWSGlueServiceRoleDefault',
      targets: {
        s3Targets: [{
          path: raw_data,
        }],
      },
      name: "landing-zone-crawler",
      tablePrefix: "raw_",
      databaseName: "etl_pipeline" //Hard-coded
    });

    // Create tables in Glue DB for Parquet data in clean zone
    const cleanZoneCrawler = new glue.CfnCrawler(this, "cleanZoneCrawler", {
      role: 'arn:aws:iam::911320291896:role/AWSGlueServiceRoleDefault',
      targets: {
        s3Targets: [{
          path: clean_data,
        }],
      },
      name: "clean-zone-crawler",
      tablePrefix: "parquet_",
      databaseName: "etl_pipeline" //Hard-coded
    });

    // Create named views and queries in Athena (CfnNamedQuery)
    const athena_order_view_1 = new athena.CfnNamedQuery(this, 'athena-view-1', {
      database: 'etl-pipeline',
      queryString: `
      CREATE OR REPLACE VIEW "raw_orders_master_view" AS 
      SELECT
        ord.*
      , emp.company employee_company
      , emp.last_name employee_last_name
      , emp.first_name employee_first_name
      , emp.job_title employee_job_title
      , emp.business_phone employee_business_phone
      , emp.home_phone employee_home_phone
      , emp.fax_number employee_fax_number
      , emp.address employee_address
      , emp.city employee_city
      , emp.state_province employee_state_province
      , emp.zip_postal_code employee_zip_postal_code
      , emp.country_region employee_country_region
      , emp.web_page employee_web_page
      , emp.notes employee_notes
      , cust.company customer_company
      , cust.job_title customer_job_title
      , cust.business_phone customer_business_phone
      , cust.fax_number customer_fax_number
      FROM
        (("etl_pipeline"."raw_orders" ord
      INNER JOIN "etl_pipeline"."raw_employee" emp ON (ord."employee_id" = emp."employee_id"))
      INNER JOIN "etl_pipeline"."raw_customer" cust ON (ord."customer_id" = cust."customer_id"))`,
      description: 'Join customer, employee, and orders together.',
      name: 'create-order-master-view',
    });

    const athena_order_view_2 = new athena.CfnNamedQuery(this, 'athena-view-2', {
      database: 'etl-pipeline',
      queryString: `
      CREATE OR REPLACE VIEW "raw_orders_master_detail_view" AS 
      SELECT
        omv.*
      , ord_det.order_detail_id
      , ord_det.quantity order_quantity
      , ord_det.unit_price order_unit_price
      , ord_det.discount order_discount
      , ord_det.status_id order_status_id
      , ord_det.purchase_order_id order_purchase_id
      , ord_det.inventory_id order_inventory_id
      , ord_det.qty_x_up
      , ord_det.sale_grades order_sale_grades
      , ord_det.product_name order_product_name
      FROM
        ("etl_pipeline"."raw_orders_master_view" omv
      INNER JOIN "etl_pipeline"."raw_order_details" ord_det ON (omv."order_id" = ord_det."order_id"))`,
      description: 'Join order_details and orders_master_view together',
      name: 'create-order-detail-master-view',
    });

    const athena_query_1 = new athena.CfnNamedQuery(this, 'athena-query-1', {
      database: 'etl-pipeline',
      queryString: `
        SELECT 
          CONCAT(employee_first_name, ' ', employee_last_name) employee_full_name, 
          SUM(order_summary) sales_total 
        FROM "etl_pipeline"."raw_orders_master_detail_view"
        GROUP BY CONCAT(employee_first_name, ' ', employee_last_name) 
        ORDER BY sales_total DESC;`,
      description: 'List total sales for each sales person in descending order',
      name: 'order-total-by-salesperson-cdk',
    });

/////////////////////////////// TESTING ///////////////////////////////////////////////////
    

    // // Glue trigger
    // const lzCrawlerSchedule: glue.CfnCrawler.ScheduleProperty = {
    //   scheduleExpression: 'cron()'
    // }

    // // Create Glue Workflow (Crawler -> Job -> Crawler)

    // // Create glue workflow for crawlers and parquet job
    // const glue_workflow = new glue.CfnWorkflow(this, "glue-workflow", {
    //   name: "glue-workflow",
    //   description:
    //     "ETL workflow to convert CSV to parquet",
    // });
    
    // // Define Glue Job to convert CSV to Parquet
    // const csv_to_parquet_glue_job = new glue.CfnJob(this, "glue-job-parquet", {
    //   name: "glue-workflow-parquetjob",
    //   description: "Convert the csv files in S3 to parquet",
    //   role: landingZoneCrawler.role,
    //   executionProperty: {
    //     maxConcurrentRuns: 1,
    //   },
    //   command: {
    //     name: "glueetl", //spark ETL job must be set to value of 'glueetl'
    //     pythonVersion: "3",
    //     scriptLocation: '',
    //   },
    //   defaultArguments: {
    //     "--TempDir": "s3://" + 'assetBucketName' + "/output/temp/",
    //     "--job-bookmark-option": "job-bookmark-disable",
    //     "--job-language": "python",
    //     "--spark-event-logs-path": "s3://" + 'assetBucketName' + "/output/logs/",
    //     "--enable-metrics": "",
    //     "--enable-continuous-cloudwatch-log": "true",
    //     "--glue_database_name": 'etl_pipeline',
    //     "--output_bucket_name": clean_data,
    //     "--output_prefix_path": 'parquet_'
    //   },
    //   maxRetries: 2,
    //   timeout: 240,
    //   numberOfWorkers: 10,
    //   glueVersion: "3.0",
    //   workerType: "G.1X",
    // });

  }
}
