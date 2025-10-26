import weaviate
from weaviate.classes.config import Configure, Property, DataType

client = weaviate.connect_to_local()

projectDB = client.collections.create(
    name="ProjectV4",
    properties=[
        Property(name="string_project", data_type=DataType.TEXT),
        Property(name="string_task", data_type=DataType.TEXT),
    ],
    vector_config=Configure.Vectors.text2vec_ollama(     # Configure the Ollama embedding integration
        api_endpoint="http://host.docker.internal:11434",       # Allow Weaviate from within a Docker container to contact your Ollama instance
        model="nomic-embed-text",                               # The model to use
    ),
    generative_config=Configure.Generative.ollama(              # Configure the Ollama generative integration
        api_endpoint="http://host.docker.internal:11434",       # Allow Weaviate from within a Docker container to contact your Ollama instance
        model="llama3.2",                                       # The model to use
    )
)

client.close()  # Free up resources