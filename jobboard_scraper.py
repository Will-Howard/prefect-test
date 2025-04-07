import time
import asyncio
from prefect import flow, suspend_flow_run, task, pause_flow_run
from prefect.blocks.system import String

this_source = 'https://github.com/Will-Howard/prefect-test.git'

@task(log_prints=True)
def process_org(org: dict) -> str:
  print(f"Processing {org['name']} at {org.get('job_board_url', 'No URL')}")
  time.sleep(1)
  result = f"✅ Completed {org['name']}"
  print(result)
  return result
import random

@flow(name="Org Subflow", log_prints=True)
async def org_subflow(org: dict) -> str:
  if random.random() < 0.1:  # 10% chance
    print(f"Pausing for approval for: {org['name']}")
    await suspend_flow_run(
      key=f"Approve processing for {org['name']}",
      timeout=60 * 60 * 24  # optional: 24hr timeout
    )
    print(f"Approval received for {org['name']} — continuing.")
  return process_org(org)

@task(log_prints=True)
async def costly_preprocessing():
    print("Starting costly pre-processing step...")
    time.sleep(5)  # Simulate a costly operation
    print("Costly pre-processing completed.")

@task(log_prints=True)
async def costly_postprocessing(results: dict):
    print("Starting costly post-processing step...")
    time.sleep(5)  # Simulate a costly operation
    print("Costly post-processing completed.")
    print("\n✅ Final results:", results)

@flow(name="Top Level Flow", log_prints=True)
async def top_flow():
    await costly_preprocessing()  # Add pre-processing step

    orgs = [
        {"id": str(i), "name": f"Org {i}", "job_board_url": f"https://example.com/org{i}/jobs"}
        for i in range(1, 101)  # Simulate ~100 orgs
    ]
    results = {}
    tasks = [org_subflow(org) for org in orgs]
    results_list = await asyncio.gather(*tasks)
    for org, result in zip(orgs, results_list):
        results[org["name"]] = result

    await costly_postprocessing(results)  # Add post-processing step

if __name__ == "__main__":
  flow.from_source(
        source=this_source,
        entrypoint="jobboard_scraper.py:top_flow", # Specific flow to run
    ).deploy(
        name="main-pipeline",
        work_pool_name="my-work-pool",
    )
