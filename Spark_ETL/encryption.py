import os
from dotenv import load_dotenv
from cryptography.fernet import Fernet
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from typing import List
from pyspark.sql.udf import UserDefinedFunction

class PIIEncryption:
    """
    Example usage:

    Initialization:
        encryption = PIIEncryption()

    Encryption of a single column:
        encrypted_df = df.withColumn("Encrypted_Address", encryption.udf_encrypt()("Address"))
        encrypted_df.show()

    Decryption of a single column:
        decrypted_df = encrypted_df.withColumn("Decrypted_Address", encryption.udf_decrypt()("Encrypted_Address"))
        decrypted_df.show()

    Encrypting multiple columns:
        encrypted_df = encryption.encrypt_columns(df, ["Address", "PhoneNumber"])
        encrypted_df.show()

    Decrypting multiple columns:
        decrypted_df = encryption.decrypt_columns(encrypted_df, ["Address", "PhoneNumber"])
        decrypted_df.show()
    """

    def __init__(self):
        load_dotenv(dotenv_path="../.env")
        self.key = os.getenv("ENCYPTION_KEY")
        self.cipher_suite = Fernet(self.key)

    def encrypt_text(self,
                     plain_text: str) -> str | None:
        if plain_text is None:
            return None
        return self.cipher_suite.encrypt(plain_text.encode()).decode()

    def decrypt_text(self,
                     encrypted_text: str) -> str | None:
        if encrypted_text is None:
            return None
        return self.cipher_suite.decrypt(encrypted_text.encode()).decode()
    
    def udf_encrypt(self) -> UserDefinedFunction:
        return udf(self.encrypt_text, StringType())
    
    def udf_decrypt(self) -> UserDefinedFunction:
        return udf(self.decrypt_text, StringType())
    
    def encrypt_columns(self,
                        df: DataFrame,
                        columns: List[str]) -> DataFrame:
        for column_name in columns:
            df = df.withColumn(column_name, self.udf_encrypt(column_name))
        return df
    
    def decrypt_columns(self,
                        df: DataFrame,
                        columns: List[str]) -> DataFrame:
        for column_name in columns:
            df = df.withColumn(column_name, self.udf_decrypt(column_name))
        return df