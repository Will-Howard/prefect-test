import time
from prefect import flow, task, pause_flow_run
from prefect.blocks.system import String

# @task(log_prints=True)
# def wait_for_approval(org: dict):
#   print(f"Pausing for approval for: {org['name']}")
#   pause_flow_run(
#     wait_for_input=String(value="Type anything to resume."),
#     name=f"Approve processing for {org['name']}",
#     timeout_seconds=60 * 60 * 24  # optional: 24hr timeout
#   )
#   print(f"Approval received for {org['name']} — continuing.")

# @task(log_prints=True)
# def process_org(org: dict) -> str:
#   print(f"Processing {org['name']} at {org.get('job_board_url', 'No URL')}")
#   time.sleep(1)
#   result = f"✅ Completed {org['name']}"
#   print(result)
#   return result

# @flow(name="Org Subflow", log_prints=True)
# def org_subflow(org: dict) -> str:
#   wait_for_approval(org)
#   return process_org(org)

# @flow(name="Top Level Flow", log_prints=True)
# def top_flow():
#   orgs = [
#     {
#       "id": "1",
#       "name": "Org A",
#       "job_board_url": "https://example.com/orgA/jobs"
#     },
#     {
#       "id": "2",
#       "name": "Org B",
#       "job_board_url": "https://example.com/orgB/jobs"
#     }
#   ]
#   results = {}
#   for org in orgs:
#     print(f"\n--- Running subflow for {org['name']} ---")
#     results[org["name"]] = org_subflow(org)
#   print("\n✅ All orgs processed:", results)

# if __name__ == "__main__":
#   # Only registers the flow for manual triggering
#   top_flow.deploy(
#     name="jobboard-scraper",
#     work_pool_name="my-work-pool"
#   )

from prefect import flow

@flow
def hello(name: str = "Marvin"):
    print(f"Hello {name}!")

if __name__ == "__main__":
    hello.deploy(
        name="my-hello-deployment",
        work_pool_name="my-work-pool"
    )
