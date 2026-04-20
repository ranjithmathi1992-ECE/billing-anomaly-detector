CREATE DATABASE hospital_billing;
USE hospital_billing;

CREATE TABLE hospital_bills (
    bill_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT,
    patient_name VARCHAR(100),
    treatment VARCHAR(100),
    department VARCHAR(50),
    bill_amount DECIMAL(10,2),
    bill_date DATE,
    doctor VARCHAR(100),
    branch VARCHAR(50)
);

CREATE TABLE insurance_claims (
    claim_id INT AUTO_INCREMENT PRIMARY KEY,
    bill_id INT,
    patient_id INT,
    insurance_company VARCHAR(100),
    claimed_amount DECIMAL(10,2),
    approved_amount DECIMAL(10,2),
    claim_date DATE,
    status VARCHAR(20)
);
