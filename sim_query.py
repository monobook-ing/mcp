import re

unit = {
    "name": "Liberty",
    "description": "A cozy two-story house in the heart of the Carpathians with panoramic windows and a private courtyard! We invite you to enjoy a comfortable stay in Bukovel. The house has a living room with a sofa and TV, a kitchen with a dining area, a laundry room, a bathroom on the ground floor and two bedrooms with their own bathrooms (shower and bathtub), TV and wardrobes. Large deck with dining table, private yard with grill and patio for fire, balcony with views. Smart lock for self entry, internet, parking.",
    "type": "Cottage",
}
accommodation = {
    "name": "Liberty",
    "city": "Bukovel",
    "state": "",
    "country": "Ukraine"
}
amenities_list = []

def normalize_text(value: object) -> str:
    if value is None:
        return ""
    return " ".join(re.split(r"\s+", str(value).strip().lower()))

searchable_blob = normalize_text(
    " ".join(
        [
            str(unit.get("name") or ""),
            str(unit.get("description") or ""),
            str(unit.get("type") or ""),
            str(accommodation.get("name") or ""),
            str(accommodation.get("city") or ""),
            str(accommodation.get("state") or ""),
            str(accommodation.get("country") or ""),
            " ".join(amenities_list),
        ]
    )
)

query1 = "cottage in bukovel"
normalized_query1 = normalize_text(query1)
print(f"Query: '{normalized_query1}' in blob? {normalized_query1 in searchable_blob}")

query2 = "cottage"
normalized_query2 = normalize_text(query2)
print(f"Query: '{normalized_query2}' in blob? {normalized_query2 in searchable_blob}")
