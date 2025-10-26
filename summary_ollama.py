import weaviate
from weaviate.classes.query import Filter

with weaviate.connect_to_local() as client:
    docs = client.collections.get("ProjectV4")

    gen = docs.generate.near_text(
        query="resumen ejecutivo del proyecto",
        limit=2,
        grouped_task="Escribe un resumen (5 líneas) con estado, fechas clave, costos y KPIs.",
        filters=Filter.by_property("string_project").equal("GatesMRI_Implementación")
    )

    # new API
    print(gen.generative.text)

