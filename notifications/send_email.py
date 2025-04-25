# Stylized send_email SDK

def send_email(to_emails, subject, body):
    print("\n=== EMAIL SENT ===")
    print(f"To: {', '.join(to_emails)}")
    print(f"Subject: {subject}")
    print(f"Body:\n{body}")
    print("==================\n")
