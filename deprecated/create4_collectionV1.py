import json
import uuid
import weaviate
from weaviate.classes.config import Property, ReferenceProperty, DataType, Configure

# ---------- Connect ----------
client = weaviate.connect_to_local()  

RESET = False

# ---------- Define schema ----------
def ensure_schema():
    existing = {c.name for c in client.collections.list_all()}

    if RESET:
        for name in ["Assignment", "Project", "Resource", "Task"]:
            if name in existing:
                client.collections.delete(name)
        existing = set()

    #Vectorizer
    vec_cfg = Configure.Vectorizer.text2vec_transformers()  # or text2vec_openai(), etc.

    if "Project" not in existing:
        client.collections.create(
            name="Project",
            vectorizer_config=vec_cfg,
            properties=[
                Property(name="project_id", data_type=DataType.TEXT),
                Property(name="project_string", data_type=DataType.TEXT),
            ],
        )

    if "Resource" not in existing:
        client.collections.create(
            name="Resource",
            vectorizer_config=vec_cfg,
            properties=[
                Property(name="resource_id", data_type=DataType.TEXT),
                Property(name="resource_string", data_type=DataType.TEXT),
            ],
        )

    if "Task" not in existing:
        client.collections.create(
            name="Task",
            vectorizer_config=vec_cfg,
            properties=[
                Property(name="task_id", data_type=DataType.TEXT),
                Property(name="task_string", data_type=DataType.TEXT),
            ],
        )

    if "Assignment" not in existing:
        client.collections.create(
            name="Assignment",
            vectorizer_config=vec_cfg,
            properties=[
                Property(name="assignment_string", data_type=DataType.TEXT),
            ],
            references=[
                ReferenceProperty(name="of_project", target_collection="Project"),
                ReferenceProperty(name="resource",   target_collection="Resource"),
                ReferenceProperty(name="task",       target_collection="Task"),
            ],
        )

ensure_schema()

projects = client.collections.get("Project")
resources = client.collections.get("Resource")
tasks = client.collections.get("Task")
assignments = client.collections.get("Assignment")

# ---------- Helpers (idempotent upserts) ----------
def upsert_project(pj):
    uid = pj["project_id"]
    try:
        projects.data.insert(
            properties={
                "project_id": uid,
                "project_string": pj["project_string"],
            },
            uuid=uid,
        )
    except weaviate.exceptions.WeaviateBaseError:
        # Already exists → update text if desired
        projects.data.update(uuid=uid, properties={"project_string": pj["project_string"]})
    return uid

def upsert_resource(res):
    uid = res["resource_id"]
    try:
        resources.data.insert(
            properties={
                "resource_id": uid,
                "resource_string": res["resource_string"],
            },
            uuid=uid,
        )
    except weaviate.exceptions.WeaviateBaseError:
        resources.data.update(uuid=uid, properties={"resource_string": res["resource_string"]})
    return uid

def upsert_task(task):
    uid = task["task_id"]
    try:
        tasks.data.insert(
            properties={
                "task_id": uid,
                "task_string": task["task_string"],
            },
            uuid=uid,
        )
    except weaviate.exceptions.WeaviateBaseError:
        tasks.data.update(uuid=uid, properties={"task_string": task["task_string"]})
    return uid

def insert_assignment(assignment_string, project_uuid, resource_uuid, task_uuid):
    aid = str(uuid.uuid4())
    assignments.data.insert(
        properties={"assignment_string": assignment_string},
        uuid=aid,
    )
    # Wire references
    assignments.data.reference_add(aid, "of_project", project_uuid)
    if resource_uuid:
        assignments.data.reference_add(aid, "resource", resource_uuid)
    if task_uuid:
        assignments.data.reference_add(aid, "task", task_uuid)
    return aid

# ---------- Load your JSON and ingest ----------
with open("Data/nested_projects.json", "r", encoding="utf-8") as f:
    projects_json = json.load(f)

for pj in projects_json:
    pj_id = upsert_project(pj)

    for a in pj.get("assignments", []):
        # Upsert resource (if present)
        res_id = None
        if "resource" in a and a["resource"]:
            res_id = upsert_resource(a["resource"])

        # Upsert task (if present)
        task_id = None
        if "task" in a and a["task"]:
            task_id = upsert_task(a["task"])

        # Insert assignment + references
        insert_assignment(
            assignment_string=a["assignment_string"],
            project_uuid=pj_id,
            resource_uuid=res_id,
            task_uuid=task_id,
        )

client.close()
