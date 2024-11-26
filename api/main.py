from fastapi import FastAPI
from starlette.responses import RedirectResponse

from api.routers import (
    sensor_router,
    datastream_router,
    measurement_router,
    authentication_router, 
    user_router,
    event_router,
    trafficlanes_router,
    walkability_router,
    bikeability_router,
    event_datastream_router,
    event_measurement_router,
    health_router)

app = FastAPI(
    title="AcDatEP",
    description="The API provides measurement data from the AcDatEP project of the NOWUM Institute at FH Aachen.",
    docs_url='/api/docs',
    redoc_url='/api/redoc',
    openapi_url='/api/openapi.json'
)


@app.get("/", include_in_schema=False)
async def root():
    """
    Redirect to the API documentation.
    """
    return RedirectResponse(url='/api/docs')

app.include_router(user_router, tags=["Users"])
app.include_router(authentication_router, tags=["Authentication"])

app.include_router(sensor_router, tags=["Sensors"])
app.include_router(datastream_router, tags=["Datastreams"])
app.include_router(measurement_router, tags=["Measurements"])
app.include_router(event_router, tags=["Events"])
app.include_router(trafficlanes_router, tags=["Trafficlanes"])
app.include_router(walkability_router, tags=["Walkability"])
app.include_router(bikeability_router, tags=["Bikeability"])

app.include_router(event_datastream_router, tags=["Event Datastreams"])
app.include_router(event_measurement_router, tags=["Event Measurements"])

app.include_router(health_router, include_in_schema=False)
