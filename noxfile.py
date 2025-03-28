import nox

@nox.session
def setup_env(session):
    """Setup virtual environment and install dependencies"""
    session.install("-r", "requirements.txt")
    session.run("python", "--version")  # Verify Python installation

@nox.session
def run_merge_excel(session):
    """Run the Excel merging script"""
    session.install("-r", "requirements.txt")
    session.run("python", "merge_excel.py", "/Users/niranjank/Downloads/updated/", "./merged_data.csv")

@nox.session
def run_airtable_upload(session):
    """Run the Airtable upload script"""
    session.install("-r", "requirements.txt")
    session.run("python", "import_to_airtable.py", "./merged_data.csv")
