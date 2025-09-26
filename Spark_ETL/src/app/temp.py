from opencage.geocoder import OpenCageGeocode

key = '8dc0fd0826f14285b12a0dda165950f0'
geocoder = OpenCageGeocode(key)

# query = f'USA, Chicago'
# results = geocoder.geocode(query)[0]
# print(results["geometry"])
# print(results["annotations"]["geohash"][:3])

results = geocoder.reverse_geocode(30.6203087, 20.9761911)[0]
print(results["annotations"]["geohash"][:3])