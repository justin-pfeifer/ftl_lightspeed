import numpy as np, csv, os, time, uuid, random

# === Constants ===
FILENAME = "contacts3.csv"
TARGET_SIZE = 5 * 1024**3  # 5 GiB
BATCH_SIZE = 10000
LOG_INTERVAL = 500_000

DOMAINS = np.array([
    'gmail.com', 'outlook.com', 'icloud.com', 'yahoo.com',
    'protonmail.com', 'zoho.com', 'example.com'
])

SUBADDRESSES = np.array([
    "news", "newsletter", "deals", "sales", "promos", "promotions", "updates", "alerts",
    "social", "media", "shopping", "cart", "travel", "flights", "bank", "finance",
    "amazon", "ebay", "etsy", "walmart", "paypal", "venmo", "stripe", "uber", "lyft",
    "doordash", "netflix", "spotify", "hulu", "disney"
])

# === Load names from column B ===
first_names = np.loadtxt('first_names.csv', dtype=str, delimiter=',', usecols=1)
last_names = np.loadtxt('last_names.csv', dtype=str, delimiter=',', usecols=1)


def generate_emails(first_names, last_names, batch_size):
    nums = np.random.randint(1000, 9999, batch_size).astype(str)
    ids = np.array([uuid.uuid4().hex[:4] for _ in range(batch_size)])
    domains = np.random.choice(DOMAINS, batch_size)

    fn_lower = np.char.lower(first_names.astype(str))
    ln_lower = np.char.lower(last_names.astype(str))

    emails = np.char.add(np.char.add(fn_lower, "."), ln_lower)
    emails = np.char.add(emails, nums)
    emails = np.char.add(emails, ids)
    emails = np.char.add(emails, "@")
    emails = np.char.add(emails, domains)

    return emails

# === E.164 Phone Number Generator ===
def generate_phones(batch_size):
    return [
        f"+1{random.randint(200,999)}{random.randint(200,999)}{random.randint(1000,9999)}"
        for _ in range(batch_size)
    ]

# === Batch contact generator ===
def generate_batch(batch_size):
    fn = np.random.choice(first_names, batch_size)
    ln = np.random.choice(last_names, batch_size)

    phones = generate_phones(batch_size)

    # Extensions (25% populated)
    ext_mask = np.random.rand(batch_size) > 0.75
    extensions = np.where(
        ext_mask,
        np.random.randint(100, 9999, batch_size).astype(str),
        None
    )

    # Subaddresses (25% populated)
    sub_mask = np.random.rand(batch_size) > 0.75
    subs = np.where(
        sub_mask,
        np.random.choice(SUBADDRESSES, batch_size),
        None
    )

    # Emails
    emails = generate_emails(fn, ln, batch_size)

    return zip(fn, ln, phones, extensions, emails, subs)

# === CSV Write Loop ===
def write_contacts():
    record_count = 0
    print("[START] Writing contacts...")

    with open(FILENAME, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['first_name', 'last_name', 'phone', 'extension', 'email', 'subaddress'])

        start_time = time.time()

        while not os.path.exists(FILENAME) or os.path.getsize(FILENAME) < TARGET_SIZE:
            batch_start = time.time()
            batch = generate_batch(BATCH_SIZE)
            writer.writerows(batch)
            record_count += BATCH_SIZE

            if record_count % LOG_INTERVAL == 0:
                mb = os.path.getsize(FILENAME) / 1024**2
                print(f"[INFO] {record_count:,} records written... {mb:.2f} MiB | Batch took {time.time() - batch_start:.2f}s")

        duration = time.time() - start_time
        gb = os.path.getsize(FILENAME) / 1024**3
        print(f"[DONE] File reached {gb:.2f} GiB with {record_count:,} records in {duration:.2f}s.")

if __name__ == "__main__":
    write_contacts()
