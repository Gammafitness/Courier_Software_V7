from . import bluedart

def get_pricing_function(courier_name):
    mapping = {
        "Bluedart": bluedart.calculate_price,
    }
    return mapping.get(courier_name, None)
