import time
import asyncio
import random
from prefect import flow, suspend_flow_run, task, pause_flow_run
from prefect.blocks.system import String

this_source = 'https://github.com/Will-Howard/prefect-test.git'

@task(log_prints=True)
def attempt_process_org(org: dict) -> bool:
    """
    Attempt to process an org. 
    If 'approval needed' is discovered mid-processing, return True.
    Otherwise return False.
    """
    print(f"Attempting to process {org['name']} ...")
    time.sleep(1)  # simulate some processing
    if random.random() < 0.1:  # 10% chance
        print(f"--> {org['name']} needs approval before finishing processing.")
        return True   # signals that approval is needed
    print(f"✅ Completed {org['name']} with no approval needed.")
    return False

@task(log_prints=True)
def finalize_process_org(org: dict) -> str:
    """
    After approval is granted, finish processing the org.
    """
    print(f"Finishing processing for {org['name']} post-approval.")
    time.sleep(1)  # simulate some additional processing
    result = f"✅ Completed {org['name']} (post-approval)."
    print(result)
    return result

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
    # Step 1: Costly pre-processing (example)
    await costly_preprocessing()

    # Create example orgs
    orgs = [
        {"id": str(i), "name": f"Org {i}", "job_board_url": f"https://example.com/org{i}/jobs"}
        for i in range(1, 101)
    ]

    # Step 2: Attempt processing each org to discover if approval is needed
    flags = [attempt_process_org.submit(org) for org in orgs]
    approvals_needed = [flag.result() for flag in flags]

    orgs_need_approval = []
    orgs_done          = []
    for org, needed in zip(orgs, approvals_needed):
        if needed:
            orgs_need_approval.append(org)
        else:
            orgs_done.append(org)

    print(f"\nOrgs done without approval: {len(orgs_done)}")
    print(f"Orgs requiring manual approval: {len(orgs_need_approval)}")

    # Step 3: If any orgs need partial approval, pause the entire flow
    if orgs_need_approval:
        print("\nPausing flow for manual approval...")
        await pause_flow_run(
            key="Manual Approval Required",
            timeout=60 * 60 * 24  # e.g. 24 hours
        )
        print("Manual approval received; resuming flow...")

    # Step 4: Finalize processing for those that needed approval
    tasks_approval = [finalize_process_org.submit(org) for org in orgs_need_approval]
    results_approval = [t.result() for t in tasks_approval]

    # Combine final results
    results_combined = {}
    for org in orgs_done:
        # orgs_done had no "approval needed," treat them as completed
        results_combined[org["name"]] = f"✅ Completed {org['name']} with no approval needed."
    for org, res in zip(orgs_need_approval, results_approval):
        results_combined[org["name"]] = res

    # Step 5: Costly post-processing (example)
    await costly_postprocessing(results_combined)

if __name__ == "__main__":
    flow.from_source(
        source=this_source,
        entrypoint="jobboard_scraper.py:top_flow", # Specific flow to run
    ).deploy(
        name="main-pipeline",
        work_pool_name="my-work-pool",
    )
