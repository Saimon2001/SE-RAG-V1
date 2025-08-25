import weaviate

with weaviate.connect_to_local() as client:
    docs = client.collections.get("Project")

    gen = docs.generate.near_text(
        query="resumen ejecutivo del proyecto GatesMRI",
        limit=4,
        grouped_task="Escribe un resumen (5 líneas) con estado, fechas clave, costos y KPIs."
    )

    # new API
    print(getattr(gen.generative, "text", gen.generative.get("groupedResult")))

