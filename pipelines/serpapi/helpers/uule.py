import base64, struct, math

# Google's "precision 8" header byte
_HDR = b'\\x00'

def uule(city:str, lat:float, lng:float) -> str:
    # Ensure lat/lng are floats
    try:
        lat = float(lat)
        lng = float(lng)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Latitude ({lat}) and Longitude ({lng}) must be valid numbers for uule generation. Error: {e}") from e
        
    loc = f"{city}".encode('utf-8')
    # Note: The original code snippet didn't use lat/lng in the payload.
    # Google's UULE generation is more complex than just the city name.
    # It involves packing lat/lon/radius/etc. in a specific proto format.
    # The provided code only encodes the city name.
    # For a *correct* UULE based on lat/lng, we would need a proper library 
    # or the full algorithm. Let's assume for now the user wants *this specific*
    # simple city name encoding, or we should use the pyuule library as suggested.
    # Reverting to just encoding the city name as per the snippet:
    payload = struct.pack('>B', len(loc)) + loc 
    return base64.urlsafe_b64encode(_HDR + payload).decode()

# Example (using the provided simple encoding logic):
if __name__ == '__main__':
    city_name = "New York City"
    latitude = 40.7128
    longitude = -74.0060
    
    # Note: This example call will use the simple city name encoding, 
    # not the actual lat/lon according to the current function logic.
    encoded_uule = uule(city_name, latitude, longitude) 
    print(f"UULE for '{city_name}' (simple name encoding): {encoded_uule}")
    
    # If using pyuule (recommended for actual lat/lon based UULE):
    # try:
    #     import pyuule
    #     proper_uule_string = pyuule.encode(latitude, longitude, role=2, producer=12, provenance=6)
    #     print(f"UULE for lat={latitude}, lng={longitude} (using pyuule): w+{proper_uule_string}") # Prepend 'w+' as required by SerpApi
    # except ImportError:
    #     print("\\nInstall 'pyuule' library to generate correct lat/lon based UULE strings.") 