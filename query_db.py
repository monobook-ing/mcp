from dotenv import load_dotenv
load_dotenv()
from db import fetch_all

def main():
    try:
        rooms = fetch_all("SELECT r.id, r.name, r.type, r.description, p.city, p.country FROM rooms r JOIN properties p ON r.property_id = p.id")
        for r in rooms:
            print(dict(r))
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
