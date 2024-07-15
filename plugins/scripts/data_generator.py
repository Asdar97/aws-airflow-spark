import random
import pandas as pd
from faker import Faker
import logging

def generate_ewallet_transactions(file_path):
    fake = Faker()
    
    # List of possible transaction output
    transaction_types = ['Payment', 'Transfer', 'Top-up', 'Withdrawal']
    purchase_categories = ['Groceries', 'Transportation', 'Dining', 'Entertainment', 'Shopping', 'Utilities', 'Health', 'Education']
    payment_methods = ['Credit Card', 'Debit Card', 'Bank Transfer', 'Cash']
    statuses = ['Completed', 'Pending', 'Failed', 'Refunded']
    
    # Generate a random number of rows between min_rows and max_rows
    num_rows = random.randint(100000, 110000)
    
    # Generate dummy data
    data = []
    for _ in range(num_rows):
        transaction_date = fake.date_between(start_date='-1y', end_date='today')
        transaction_time = fake.time()
        amount = round(random.uniform(1, 1000), 2)
        transaction_type = random.choice(transaction_types)
        purchase_category = random.choice(purchase_categories) if transaction_type == 'Payment' else ''
        balance_before = round(random.uniform(0, 10000), 2)
        balance_after = round(balance_before + amount if transaction_type in ['Top-up', 'Transfer'] else balance_before - amount, 2)
        
        data.append({
            'Transaction_ID': fake.uuid4(),
            'Date': transaction_date,
            'Time': transaction_time,
            'User_ID': fake.uuid4(),
            'Amount': amount,
            'Transaction_Type': transaction_type,
            'Purchase_Category': purchase_category,
            'Merchant': fake.company() if transaction_type == 'Payment' else '',
            'Payment_Method': random.choice(payment_methods) if transaction_type in ['Payment', 'Top-up'] else '',
            'Status': random.choice(statuses),
            'Balance_Before': balance_before,
            'Balance_After': balance_after,
            'Description': f"{transaction_type} for {purchase_category}" if transaction_type == 'Payment' else f"{transaction_type} transaction"
        })
    
    # Create dummy data as DataFrame
    df = pd.DataFrame(data)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(['Date', 'Time'])

    df.to_csv(file_path, index=False)
    
    logging.info(f"Generated {num_rows} rows of e-wallet transaction data in {file_path}")