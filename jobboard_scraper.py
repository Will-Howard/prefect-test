import time
import asyncio
from prefect import flow, task, pause_flow_run
from prefect.blocks.system import String

this_source = 'https://github.com/Will-Howard/prefect-test.git'

@task(log_prints=True)
def process_org(org: dict) -> str:
  print(f"Processing {org['name']} at {org.get('job_board_url', 'No URL')}")
  time.sleep(1)
  result = f"✅ Completed {org['name']}"
  print(result)
  return result

@flow(name="Org Subflow", log_prints=True)
async def org_subflow(org: dict) -> str:
  print(f"Pausing for approval for: {org['name']}")
  await pause_flow_run(
    key=f"Approve processing for {org['name']}",
    timeout=60 * 60 * 24  # optional: 24hr timeout
  )
  print(f"Approval received for {org['name']} — continuing.")
  return process_org(org)

@flow(name="Top Level Flow", log_prints=True)
async def top_flow():
  orgs = [
    {
      "id": "1",
      "name": "Org A",
      "job_board_url": "https://example.com/orgA/jobs"
    },
    {
      "id": "2",
      "name": "Org B",
      "job_board_url": "https://example.com/orgB/jobs"
    }
  ]
  results = {}
  tasks = [org_subflow(org) for org in orgs]
  results_list = await asyncio.gather(*tasks)
  for org, result in zip(orgs, results_list):
    results[org["name"]] = result
  print("\n✅ All orgs processed:", results)

if __name__ == "__main__":
  flow.from_source(
        source=this_source,
        entrypoint="jobboard_scraper.py:top_flow", # Specific flow to run
    ).deploy(
        name="main-pipeline",
        work_pool_name="my-work-pool",
    )
