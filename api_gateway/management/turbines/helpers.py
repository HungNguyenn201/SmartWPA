"""Turbine-specific helper functions"""
def get_turbine_serialized_data(turbine):
    """Serialize turbine data for response"""
    return {
        "id": turbine.id,
        "name": turbine.name,
        "capacity": turbine.capacity,
        "latitude": turbine.latitude,
        "longitude": turbine.longitude,
        "is_active": turbine.is_active,
        "farm": {
            "id": turbine.farm.id,
            "name": turbine.farm.name
        } if turbine.farm else None,
        "investor": {
            "id": turbine.farm.investor.id,
            "name": turbine.farm.investor.name
        } if turbine.farm and turbine.farm.investor else None,
        "created_at": turbine.time_created.isoformat() if turbine.time_created else None,
        "last_data_update": turbine.last_data_update.isoformat() if turbine.last_data_update else None
    }

