from flask import Flask, render_template, url_for, redirect, flash, session, request
from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.gapic.transports import job_controller_grpc_transport
from google.cloud import storage
import os

app = Flask(__name__) 

SECRET_KEY='development key'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']="/app/""My First Project-08b19c4be7a6.json"""

app.config.from_object(__name__)
app.config.from_envvar('EJ_SEARCH_ENGINE', silent=True)



@app.route('/') 
def hello():
	return render_template("choose.html")

@app.route('/loaded/', methods=['GET', 'POST'])
def loaded():
	if request.method == 'POST':
		# Get folder
		fileArr=[]
		folder = []
		files = request.files.getlist('file')
		for f in files:
			fileArr.append(f.filename)
		folder = fileArr[0].split("/")

		# Dataproc API
		transport = job_controller_grpc_transport.JobControllerGrpcTransport(address='us-west1-dataproc.googleapis.com:443')
		project_id = 'imperial-sphere-273422'
		region = 'us-west1'
		# Define Job arguments:
		job_args = []
		job_args.append('gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/' + folder[0])
		job_args.append('gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/new')
		job_client = dataproc_v1.JobControllerClient(transport)
		# Create Hadoop Job
		hadoop_job = dataproc_v1.types.HadoopJob(jar_file_uris=['gs://dataproc-staging-us-west1-628394627960-6e5uyn8v/JAR/invertedindex.jar'], main_class='InvertedIndex', args=job_args)
		# Define Remote cluster to send Job
		job_placement = dataproc_v1.types.JobPlacement()
		job_placement.cluster_name = 'cluster-f010'
		# Define Job configuration
		main_job = dataproc_v1.types.Job(hadoop_job=hadoop_job, placement=job_placement)
		# Send job
		result = job_client.submit_job(project_id, region, main_job)
		job_id = result.reference.job_id

		"""Wait for job to complete or error out."""
		while True:
			job = job_client.get_job(project_id, region, job_id)
			if job.status.State.Name(job.status.state) == 'DONE':
				return render_template("loaded.html")
	return render_template("loaded.html")

@app.route('/loaded/search/')
def search():
	return render_template("search.html")

@app.route('/loaded/searched/', methods=['GET', 'POST'])
def searched():
	if request.method == 'POST':
		if request.form['term']:
			term = request.form['term']
			# *start before call to GCP and stop when value returns*
			time = 0
			cars = [{"car":"ford", "year":"2"}, {"car":"rari", "year":"3"}]
			return render_template("searched.html", term=term, time=time, rows=cars)
	return render_template("searched.html")

@app.route('/loaded/topn/')
def topn():
	return render_template("topn.html")

@app.route('/loaded/indicies/', methods=['GET', 'POST'])
def indicies():
	if request.method == 'POST':
		if request.form['term']:
			flash('Inverted indicies were created successfully!')
			term = request.form['term']
			# *start before call to GCP and stop when value returns*
			time = 0
			cars = [{"car":"ford", "year":"2"}, {"car":"rari", "year":"3"}]
			return render_template("indicies.html", term=term, time=time, rows=cars)
	return render_template("indicies.html")
  
  
if __name__ == "__main__":
    app.run(host = '0.0.0.0', debug = True)
    # app.run(debug = True)

