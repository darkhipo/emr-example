# An Example for how to develop and deploy EMR jobs
This is a brief description and example of how to develop and bring into development and production EMR jobs.

## Zeppelin
The first step in developing a project in EMR, making use of Hadoop, Spark, etc, is to get it running in Zeppelin. Zeppelin is a web front-end to EMR, it is much like Jupyter notebooks for ML in python. Zeppelin takes away many of the annoyances of working in EMR, so it is a good place to start working on a project, as it lets the developer get directly to the kernel of what they want to do. To develop in Zeppelin, we will need to launch an EMR cluster configured to support Zeppelin. Unfortunately, the default way to do this provided by AWS (through the web console) is broken and will launch a failining instance. Instead we will use terraform to start up the instance,Notice that the script we use to start the cluster (`emr_example.tf`) requires certain AWS parameters that will need to get filled in, mostly this means security identifiers like ARNS.

You'll need to:
1. [Install Terraform][itf].
2. Modify `ec2_attributes` in `emr_example.tf` to map to properly configured security groups in AWS (you'll need to make these). 
3. Run the command `${TERRAFORM_HOME}/terraform apply -var launch_date=$(date '+%y%m%d%H%M')` from inside the directory containing `emr_example.tf` to start the cluster up. Make sure you have `TERRAFORM_HOME` set in your environment, it is not done as part of the terraform installation, you have to set it. 
4. Find the public URL of the ec2 instance running Zeppelin (let's say it is `MY_AWS_URL`) and navigate a web-browser to `http://MY_AWS_URL:8890` (Zeppelin by default uses port 8890). 
5. You can now develop your algorithms on the Zeppelin instance running on the cluster. 
6. Don't forget to shut down the cluster (you can do this through the web console) it is expensive to keep running all the time!

## Submitting ECS Jobs
After developing the distributed program on ECS in Zeppelin, you'll want to be able to launch it outside of Zeppelin, either triggered by some event or manually. **Warning:** unmodified Zepellin code WILL NOT RUN when launched from an ec2 server in an ECR cluster.  

### An Example
Let's use the following scenario as an example to illustrate how to performa an EMR calculation: Suppose that a large database is being filled up, it's getting larger day by day. Suppose further that every day the database is backed up as a block of text files containing plain text. We want to use EMR to take two such consecutive backups an perform a *diff* operation on them, that is we want to store all records in the later backup that are absent from the earlier backup. 

To accomplish this task we're going to use the boto aws client to start and configure an EMR cluster, run the python diff code against the database backups (stored on AWS S3) and then write the result of the diff (also to S3). To make this happen we need to:
1. Upload a bootstrap shell script (`emr-bootstrap.sh`) to s3.
2. Upload the python script (`mirror_delta.py`) to perform the diff operation using pyspark.
3. Launch an aws emr task sequence on EMR to load up an appropriately provisioned EMR cluster, bootstrapping each node in the cluster as specified in the `emr-bootstrap.sh` script, then launch the execution of `mirror_delta.py`, passing in the needed arguments, the finally teardown the EMR cluster at the end of the computation.

Running `emr_scripts_to_s3.py` will upload `emr-bootstrap.sh` and `mirror_delta.py` to s3. Then, running `emr_incr_unloads.py` will initiate the sequence to start up an EMR cluster, perform the diff calculation using it, then shut it down. `deploy_emr.py` is used by `emr_incr_unloads.py`; `deploy_emr.py` contains the more cluster specific part of the configuration, whereas  `emr_incr_unloads.py` contains the task specific (performing the diff operation) parts of the configuration. 

So to perform the full calculation one could run: `python -c 'emr_scripts_to_s3.main(); emr_incr_unloads.main()'`

[itf]: https://www.vasos-koupparis.com/terraform-getting-started-install/
