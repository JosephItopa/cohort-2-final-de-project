#### CDE COHORT-2: CAPSTONE PROJECT

#### Project overview/background
##### Coretelecoms is a company based in US, and they operate in the Telecom space. The company's challenge rest in the area of customer retention, which is critical to the business. Customers have series of challenges which the company collates through several channels:
##### 1. social media complaints
##### 2. Call center log files

##### 3. Website forms 

##### Other challenges includes:
##### 1. The files are stored in different formats.

##### 2. The files are located at different sources.
#### Project structure
##### COHORT-2-FINAL-PROJECT/
##### │
##### ├── .github/
##### │   └── workflows/
##### │       └── python-script.yaml
##### │
##### ├── slide/
##### │   (folder with presentation)
##### │
##### ├── extraction/
##### │   └── extract.py
##### │   └── extract_ingest.py
##### ├── transformation/
##### │   └── tranform.py
##### ├── data_loader/
##### │   └── load.py
##### ├── notebooks/
##### │   └── pipe.py
##### ├── main.tf
##### ├── pipeline.py
##### ├── README.md
##### ├── requirements.txt
##### └── variables.tf

##### COMPONENT BREAKDOWN
##### A) Infrastructure
##### main.tf
##### variables.tf

##### B) Github Action
##### .github/workflows/python-script.yaml

##### C) ETL Pipeline Code
##### extraction/extract.py           # Handles data extraction logic
##### transformation/transform.py         # Cleans/transforms data
##### data_loader/load.py              # Loads data (e.g., to Supabase)
##### notebooks/pipeline.py          # Orchestration of entire ETL pipeline

##### d) Docs
##### README.md
##### requirements.txt
##### slide/


#### Architecture diagram

#### How to Run the Project

#### Choice of tools and technology.
##### 1. Python
##### 2. SQL
##### 3. AWS, GCP
##### 4. Github Action
##### 5. Supabase
#### How to set up the project

##### A) Git clone the repo: git clone git@github.com:JosephItopa/cohort-2-final-de-project.git
##### B) Navigate to cohort-2-final-de-project
##### C) Export the credentials for Terraform. Examples:
###### export AWS_ACCESS_KEY_ID="ASIAxxxx"
###### export AWS_SECRET_ACCESS_KEY="xxxx" 
###### export AWS_SESSION_TOKEN="xxxx"
##### After escorting the credentials sequentially and run the command below:
##### 1)terraform init
##### 2)terraform plan -var="bucket_name=my-bucket" -var="folder_name=raw"
##### 3) terraform apply -auto-approve

##### D) The trigger the 'run flow' button on github and it will run.
