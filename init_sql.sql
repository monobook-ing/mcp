-- ============================================================
-- Hotel Booking — Supabase Init SQL
-- ============================================================

-- 1. Properties (hotels/accommodations)
CREATE TABLE properties (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id UUID,
    street TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    floor TEXT,
    section TEXT,
    property_number TEXT,
    description TEXT,
    image_url TEXT,
    rating TEXT,
    ai_match_score TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 2. Rooms (units/cottages/penthouses)
CREATE TABLE rooms (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id UUID NOT NULL REFERENCES properties(id),
    name TEXT NOT NULL,
    type TEXT,
    description TEXT,
    images TEXT[] DEFAULT '{}',
    price_per_night DECIMAL(10,2) NOT NULL,
    max_guests INTEGER NOT NULL,
    bed_config TEXT,
    amenities TEXT[] DEFAULT '{}',
    source TEXT,
    source_url TEXT,
    sync_enabled BOOLEAN DEFAULT FALSE,
    last_synced TIMESTAMPTZ,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    currency_code TEXT DEFAULT 'USD'
);

-- 3. Guests
CREATE TABLE guests (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id UUID NOT NULL REFERENCES properties(id),
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    notes TEXT DEFAULT '',
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 4. Bookings
CREATE TABLE bookings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id UUID NOT NULL REFERENCES properties(id),
    room_id UUID NOT NULL REFERENCES rooms(id),
    guest_id UUID NOT NULL REFERENCES guests(id),
    check_in DATE NOT NULL,
    check_out DATE NOT NULL,
    guests_count INTEGER DEFAULT 1,
    total_price DECIMAL(10,2),
    status TEXT DEFAULT 'confirmed',
    ai_handled BOOLEAN DEFAULT TRUE,
    source TEXT DEFAULT 'chatgpt',
    conversation_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    cancelled_at TIMESTAMPTZ,
    currency_code TEXT DEFAULT 'USD'
);

-- Indexes
CREATE INDEX idx_rooms_property ON rooms(property_id);
CREATE INDEX idx_bookings_room ON bookings(room_id);
CREATE INDEX idx_bookings_guest ON bookings(guest_id);
CREATE INDEX idx_properties_city ON properties(city);

-- ============================================================
-- Seed Data
-- ============================================================

-- Property: Vysota890
INSERT INTO properties (id, account_id, street, city, state, postal_code, country, lat, lng, floor, section, property_number, description, image_url, rating, ai_match_score)
VALUES (
    '22e693ca-deb8-4550-a4b1-9480f36cbb38',
    NULL,
    '789 Alpine Road',
    'Tirol',
    'Lvivska',
    '82663',
    'Ukraine',
    48.77998,
    23.42817,
    'Ground',
    'Main Lodge',
    'ML-01',
    'Charming alpine lodge with mountain views and ski access',
    'https://apps-sdk-beta.vercel.app/apps/assets/vysota22.jpeg',
    '4.70',
    NULL
);

-- Room 1: Sova House - Height 890
INSERT INTO rooms (id, property_id, name, type, description, images, price_per_night, max_guests, bed_config, amenities, currency_code)
VALUES (
    '1685a254-bdf7-4b57-882b-9d9691b10fad',
    '22e693ca-deb8-4550-a4b1-9480f36cbb38',
    'Sova House - Height 890',
    'Cottage',
    'The largest of the three houses - Sova is perfect for a group of 4-6 people. We invite you to relax in a luxurious cottage in the heart of the mountains with panoramic windows overlooking majestic views. The cottage combines stylish design and mountain comfort. The complex has a Jacuzzi, spa, fireplace, BBQ grill and vinyl player to create an atmosphere. For those who love movies, there is a projector. Perfect place to recharge and relax with family or friends. Nearby is Zakhar Berkut and a lift on Mount Ilza.',
    ARRAY['https://apps-sdk-beta.vercel.app/apps/assets/vysota11.jpeg','https://apps-sdk-beta.vercel.app/apps/assets/vysota12.webp','https://monobook.s3.us-east-1.amazonaws.com/vysota13.avif','https://monobook.s3.us-east-1.amazonaws.com/vysota14.avif','https://monobook.s3.us-east-1.amazonaws.com/vysota15.avif','https://monobook.s3.us-east-1.amazonaws.com/vysota16.avif','https://monobook.s3.us-east-1.amazonaws.com/vysota17.avif','https://monobook.s3.us-east-1.amazonaws.com/vysota18.avif'],
    180.00,
    6,
    '2 bedrooms · 2 beds · 2 baths',
    ARRAY['Kitchen','Wifi','Free parking on premises','Pool - available seasonally','Hot tub','Sauna','EV charger - level 2','Washer','Exterior security cameras on property','Hair dryer','Cleaning products','Yasen shampoo','Yasen body soap','Bidet','Hot water','Dryer – In building','Hangers','Bed linens','Extra pillows and blankets','Room-darkening shades','Iron','Clothing storage: closet','Record player','Books and reading material','Movie theater','Air conditioning','Indoor fireplace','Heating','Carbon monoxide alarm','Fire extinguisher','First aid kit','Refrigerator','Microwave','Dishes and silverware','Mini fridge','Dishwasher','Other induction stove','Hot water kettle','Coffee maker','Wine glasses','Barbecue utensils','Dining table','Coffee','Private entrance','Laundromat nearby','Private patio or balcony','Shared backyard','Fire pit','Outdoor dining area','Private outdoor kitchen','BBQ grill','Single level home','Luggage dropoff allowed','Breakfast','Cleaning available during stay'],
    'USD'
);

-- Room 2: Sparrow - Vysota890
INSERT INTO rooms (id, property_id, name, type, description, images, price_per_night, max_guests, bed_config, amenities, currency_code)
VALUES (
    '53c3528a-aabb-41f6-88a2-5993638c7a40',
    '22e693ca-deb8-4550-a4b1-9480f36cbb38',
    'Sparrow - Vysota890',
    'Cottage',
    'We invite you to relax in a luxurious cottage in the mountains with panoramic windows that open up a wonderful view. There is a Jacuzzi, a spa, a fireplace, and a terrace with a barbecue grill for outdoor evenings. To create a special atmosphere — a vinyl player and a projector for movies. It''s the perfect place to relax surrounded by nature, comfort, and elegance with loved ones or friends. The first of the three cottages has barrier-free access, the closest to the parking lot and SPA',
    ARRAY['https://apps-sdk-beta.vercel.app/apps/assets/vysota21.jpeg','https://apps-sdk-beta.vercel.app/apps/assets/vysota22.jpeg','https://a0.muscache.com/im/pictures/miso/Hosting-1327674972431618364/original/55d1ee1d-65ac-47a6-bdcf-66b0c1ae2802.jpeg','https://a0.muscache.com/im/pictures/miso/Hosting-1327674972431618364/original/0d33837f-a774-49e2-8984-bad56992c714.jpeg','https://a0.muscache.com/im/pictures/miso/Hosting-1327674972431618364/original/b8f05c1b-ac86-4aa7-8b46-1bf6b2b5cb96.jpeg','https://a0.muscache.com/im/pictures/miso/Hosting-1327674972431618364/original/d9d1a0f7-c710-4ddf-95ee-f8996c5dcaeb.jpeg','https://a0.muscache.com/im/pictures/miso/Hosting-1327674972431618364/original/7f0e0e9a-8938-4a05-aa06-77df2ac51aff.jpeg'],
    150.00,
    4,
    '1 bedroom · 2 beds · 1 bath',
    ARRAY['Mountain view','River view','Kitchen','Wifi','Free parking on premises','Pool','Hot tub','Sauna','Exterior security cameras on property','Pool view','Valley view','Bathtub','Hair dryer','Cleaning products','Shampoo','Body soap','Bidet','Hot water','Washer','Dryer','Essentials','Hangers','Bed linens','Room-darkening shades','Drying rack for clothing','Clothing storage','TV','Record player','Sound system','Books and reading material','Movie theater','Board games','Air conditioning','Indoor fireplace','Heating','Carbon monoxide alarm','Fire extinguisher','First aid kit','Refrigerator','Microwave','Cooking basics','Dishes and silverware','Mini fridge','Dishwasher','Stove','Hot water kettle','Coffee maker','Wine glasses','Barbecue utensils','Dining table','Coffee','Private entrance','Laundromat nearby','Resort access','Patio or balcony','Backyard','Fire pit','Outdoor dining area','Outdoor kitchen','BBQ grill','Free street parking','EV charger','Single level home','Cleaning available during stay'],
    'UAH'
);

-- Room 3: Presidential Penthouse2
INSERT INTO rooms (id, property_id, name, type, description, images, price_per_night, max_guests, bed_config, amenities, currency_code)
VALUES (
    '89a3360d-a3bc-4b77-b0ae-8d9b142e2e5c',
    '22e693ca-deb8-4550-a4b1-9480f36cbb38',
    'Presidential Penthouse2',
    'Penthouse',
    'The crown jewel of our property. A stunning two-floor penthouse with 360-degree city views, private jacuzzi, chef''s kitchen, and dedicated butler service.',
    ARRAY['https://monobook-beta.vercel.app/assets/hotel-2-061OTpbs.jpg','https://monobook-beta.vercel.app/assets/hotel-4-cWquiUJH.jpg','https://monobook-beta.vercel.app/assets/hotel-1-CLEf72jW.jpg','https://monobook-beta.vercel.app/assets/hotel-3-CfwzuMyX.jpg'],
    750.00,
    6,
    '1 King Bed + 2 Single Beds',
    ARRAY['WiFi','City View','Jacuzzi','Kitchen','Butler Service','AC','Mini Bar','Gym Access','Spa Access'],
    'USD'
);

-- Room 4: Leleka - Height 890
INSERT INTO rooms (id, property_id, name, type, description, images, price_per_night, max_guests, bed_config, amenities, currency_code)
VALUES (
    'b5c8dd8d-f833-4536-934f-1fc8c46a9fd5',
    '22e693ca-deb8-4550-a4b1-9480f36cbb38',
    'Leleka - Height 890',
    'Cottage',
    'We invite you to relax in a luxurious cottage in the mountains with panoramic windows that open up a wonderful view. There is a Jacuzzi, a spa, a fireplace, and a terrace with a barbecue grill for outdoor evenings. To create a special atmosphere — a vinyl player and a projector for movies. It''s the perfect place to relax surrounded by nature, comfort, and elegance with loved ones or friends.',
    ARRAY['https://apps-sdk-beta.vercel.app/apps/assets/vysota31.jpeg','https://apps-sdk-beta.vercel.app/apps/assets/vysota32.webp','https://a0.muscache.com/im/pictures/hosting/Hosting-U3RheVN1cHBseUxpc3Rpbmc6MTMzMjQ1OTI3NDM0MzY0NDMyMQ%3D%3D/original/60f94408-5bf6-474b-a099-de35c5c9d734.jpeg','https://a0.muscache.com/im/pictures/hosting/Hosting-U3RheVN1cHBseUxpc3Rpbmc6MTMzMjQ1OTI3NDM0MzY0NDMyMQ%3D%3D/original/36317f7f-8887-4caa-891e-c64e4d2bdc1d.jpeg','https://a0.muscache.com/im/pictures/miso/Hosting-1327449793087882342/original/be498a79-6790-4763-84a0-4825a6bd58f8.jpeg','https://a0.muscache.com/im/pictures/hosting/Hosting-U3RheVN1cHBseUxpc3Rpbmc6MTMzMjQ1OTI3NDM0MzY0NDMyMQ%3D%3D/original/6d302b3a-4fd3-4258-8752-95b393be1888.jpeg','https://a0.muscache.com/im/pictures/miso/Hosting-1327449793087882342/original/4280cde4-7777-4ca0-8b95-73b956015a6e.jpeg','https://a0.muscache.com/im/pictures/hosting/Hosting-U3RheVN1cHBseUxpc3Rpbmc6MTMzMjQ1OTI3NDM0MzY0NDMyMQ%3D%3D/original/48006b53-6eb4-4c7f-8543-dee90b605a72.jpeg'],
    150.00,
    4,
    '1 bedroom · 2 beds · 1 bath',
    ARRAY['Kitchen','Wifi','Free parking on premises','Pool','Hot tub','Sauna','TV','EV charger','Exterior security cameras on property','Bathtub','Hair dryer','Cleaning products','Shampoo','Body soap','Bidet','Hot water','Washer','Dryer','Essentials','Hangers','Bed linens','Room-darkening shades','Drying rack for clothing','Clothing storage','Record player','Sound system','Books and reading material','Movie theater','Board games','Air conditioning','Indoor fireplace','Heating','Carbon monoxide alarm','Fire extinguisher','First aid kit','Refrigerator','Microwave','Cooking basics','Dishes and silverware','Mini fridge','Dishwasher','Stove','Hot water kettle','Coffee maker','Wine glasses','Barbecue utensils','Dining table','Coffee','Private entrance','Laundromat nearby','Resort access','Patio or balcony','Backyard','Fire pit','Outdoor dining area','Outdoor kitchen','BBQ grill','Free street parking','Single level home','Cleaning available during stay'],
    'USD'
);
