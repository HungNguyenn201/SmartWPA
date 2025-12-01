"""Farm-specific helper functions"""
def get_farm_serialized_data(farm):
    """Serialize farm data for response"""
    return {
        "id": farm.id,
        "name": farm.name,
        "address": farm.address,
        "capacity": farm.capacity,
        "latitude": farm.latitude,
        "longitude": farm.longitude,
        "investor": {
            "id": farm.investor.id,
            "name": farm.investor.name
        } if farm.investor else None,
        "created_at": farm.time_created.isoformat() if farm.time_created else None
    }

