from faker import Faker
import pandas as pd
import random
from datetime import datetime, timedelta

fake = Faker()
random.seed(42)
Faker.seed(42)

DEPARTMENTS = ["Cardiology", "Orthopedics", "Neurology", "Oncology", "Pediatrics", "Gynecology", "Dermatology", "Emergency"]

PROCEDURES = {
    "Cardiology": ["Angioplasty", "ECG", "Echo Test", "Bypass Surgery"],
    "Orthopedics": ["Knee Replacement", "Hip Surgery", "Fracture Repair", "Arthroscopy"],
    "Neurology": ["MRI Brain", "EEG", "Spine Surgery", "CT Scan"],
    "Oncology": ["Chemotherapy", "Radiation", "Biopsy", "PET Scan"],
    "Pediatrics": ["Vaccination", "Neonatal Care", "Growth Check", "Fever Treatment"],
    "Gynecology": ["C-Section", "Normal Delivery", "Hysterectomy", "Ultrasound"],
    "Dermatology": ["Skin Biopsy", "Laser Treatment", "Mole Removal", "Allergy Test"],
    "Emergency": ["Trauma Care", "ICU Admission", "Ventilator", "Blood Transfusion"],
}

INSURERS = ["StarHealth", "NationalInsurance", "UnitedHealth", "BajajAllianz", "HDFCErgo", "NewIndiaAssurance"]

HOSPITALS = ["Apollo Salem", "Kaveri Hospital", "SKS Hospital", "Vinayaga Hospital"]

def generate_billing_data(n=100000):
    bills = []
    claims = []
    base_date = datetime(2024, 1, 1)

    for i in range(n):
        dept = random.choice(DEPARTMENTS)
        procedure = random.choice(PROCEDURES[dept])
        bill_amount = round(random.uniform(500, 250000), 2)
        admit_date = (base_date + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")
        patient_id = f"PAT{str(i+1).zfill(6)}"
        bill_id = f"BILL{str(i+1).zfill(6)}"

        anomaly_roll = random.random()
        if anomaly_roll < 0.10:
            claim_amount = round(bill_amount * random.uniform(0.3, 0.55), 2)
            anomaly = "HIGH"
        elif anomaly_roll < 0.30:
            claim_amount = round(bill_amount * random.uniform(0.56, 0.74), 2)
            anomaly = "MEDIUM"
        else:
            claim_amount = round(bill_amount * random.uniform(0.75, 0.98), 2)
            anomaly = "NORMAL"

        bills.append({
            "bill_id": bill_id,
            "patient_id": patient_id,
            "patient_name": fake.name(),
            "admit_date": admit_date,
            "department": dept,
            "procedure": procedure,
            "doctor": f"Dr. {fake.last_name()}",
            "days_admitted": random.randint(1, 30),
            "bill_amount": bill_amount,
            "hospital": random.choice(HOSPITALS),
        })

        claims.append({
            "claim_id": f"CLM{str(i+1).zfill(6)}",
            "bill_id": bill_id,
            "patient_id": patient_id,
            "insurer": random.choice(INSURERS),
            "policy_number": fake.bothify("POL-####-????").upper(),
            "claim_amount": claim_amount,
            "claim_date": admit_date,
            "status": random.choice(["Approved", "Pending", "Rejected"]),
            "anomaly_label": anomaly,
        })

    return pd.DataFrame(bills), pd.DataFrame(claims)


if __name__ == "__main__":
    print("Generating 100,000 hospital billing records...")
    bills_df, claims_df = generate_billing_data(100000)
    bills_df.to_csv("hospital_bills_100k.csv", index=False)
    claims_df.to_csv("insurance_claims_100k.csv", index=False)
    print(f"Done!")
    print(f"hospital_bills_100k.csv: {len(bills_df)} records")
    print(f"insurance_claims_100k.csv: {len(claims_df)} records")
    print(f"Anomaly distribution:")
    print(claims_df["anomaly_label"].value_counts().to_string())
