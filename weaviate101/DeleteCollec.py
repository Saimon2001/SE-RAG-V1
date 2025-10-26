import weaviate

client = weaviate.connect_to_local(port=8080)

class_to_delete = "Task_CR"

client.collections.delete(class_to_delete)

print(f"Deleted class: {class_to_delete}")
client.close()