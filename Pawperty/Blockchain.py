import hashlib
import json
import os
import sqlite3
import re
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any


class Owner:
    """Represents a property owner with identity details."""
    def __init__(self, name: str, aadhar: str, pan: str):
        self.name = name.strip().upper()
        self.aadhar = aadhar.replace(" ","").replace("-","").upper()
        self.pan = pan.upper()
        self.customer_key = self._generate_customer_key()

    def _generate_customer_key(self) -> str:
        unique_id = str(uuid.uuid4()).upper().replace('-', '')[:8]
        return f"CUST-{unique_id}"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "aadhar": self.aadhar,
            "pan": self.pan,
            "customer_key": self.customer_key
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> 'Owner':
        """Reconstruct an Owner from a saved dict without generating a new customer_key."""
        owner = cls.__new__(cls)
        owner.name = d["name"]
        owner.aadhar = d["aadhar"]
        owner.pan = d["pan"]
        owner.customer_key = d["customer_key"]
        return owner


class Block:
    """Represents a single block in the property blockchain."""
    
    def __init__(self, index: int, timestamp: str, data: Dict[str, Any], 
                 previous_hash: str, property_key: str):
        self.index = index
        self.timestamp = timestamp
        self.data = data
        self.previous_hash = previous_hash
        self.property_key = property_key
        self.hash = self.calculate_hash()
    
    def calculate_hash(self) -> str:
        """Calculate SHA-256 hash of the block."""
        block_string = json.dumps({
            "index": self.index,
            "timestamp": self.timestamp,
            "data": self.data,
            "previous_hash": self.previous_hash,
            "property_key": self.property_key
        }, sort_keys=True)
        return hashlib.sha256(block_string.encode()).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert block to dictionary representation."""
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "data": self.data,
            "previous_hash": self.previous_hash,
            "property_key": self.property_key,
            "hash": self.hash
        }


class PropertyBlockchain:
    """Blockchain-based property ledger system."""
    
    DB_FILE = "pawperty_ledger.db"
    STATUS_VALID = "VALID"
    STATUS_CORRECTED = "CORRECTED"
    STATUS_DISPUTED = "DISPUTED"
    STATUS_REVOKED = "REVOKED"
    
    def __init__(self):
        self.chain: List[Block] = []
        self.property_index: Dict[str, List[int]] = {}
        self.owner_registry: Dict[str, Owner] = {}  # Maps customer_key to Owner instance
        self.aadhar_to_owner: Dict[str, str] = {}
        self.pan_to_owner: Dict[str, str] = {}
        self.customer_key_to_owner: Dict[str, str] = {}
        self.survey_to_property: Dict[str, str] = {}
        self._init_db()
        if self._load_blockchain():
            print("✓ Loaded existing blockchain from database")
        else:
            self._create_genesis_block()
            print("✓ Created new blockchain with genesis block")
    
    def _create_genesis_block(self) -> None:
        """Create the first block in the chain."""
        genesis_block = Block(
            index=0,
            timestamp=datetime.now().isoformat(),
            data={
                "type": "genesis",
                "message": "Property Ledger Genesis Block",
                "status": self.STATUS_VALID,
                "transaction_id": self._tx_id_for_index(0),
            },
            previous_hash="0",
            property_key="GENESIS"
        )
        self.chain.append(genesis_block)
    
    def get_latest_block(self) -> Block:
        """Return the most recent block in the chain."""
        return self.chain[-1]

    @staticmethod
    def _tx_id_for_index(index: int) -> str:
        """Create a stable, human-readable transaction id from a block index."""
        return f"TX{1000 + index}"

    def _next_transaction_id(self) -> str:
        """Return the transaction id for the block that would be appended next."""
        return self._tx_id_for_index(len(self.chain))

    def _property_correction_targets(self, property_key: str) -> Dict[str, str]:
        """Map corrected transaction ids to the correction transaction id for a property."""
        targets: Dict[str, str] = {}
        for idx in self.property_index.get(property_key, []):
            block = self.chain[idx]
            if block.data.get("type") != "correction":
                continue
            if block.data.get("status", self.STATUS_VALID) != self.STATUS_VALID:
                continue
            previous_id = block.data.get("previous_transaction_id", "")
            if not previous_id:
                continue
            correction_tx_id = block.data.get("transaction_id", self._tx_id_for_index(block.index))
            targets[previous_id] = correction_tx_id
        return targets

    def get_property_transaction_by_id(self, property_key: str, transaction_id: str) -> Dict[str, Any]:
        """Return a property transaction by transaction id."""
        tx_id = transaction_id.strip().upper()
        for record in self.get_property_history(property_key):
            if record.get("transaction_id") == tx_id:
                return record
        raise ValueError(f"Transaction '{transaction_id}' not found for property '{property_key}'.")
    
    def validate_aadhar(self, aadhar: str) -> bool:
        """Validate Aadhar number format (12 digits)."""
        aadhar_clean = aadhar.replace(" ", "").replace("-", "")
        return len(aadhar_clean) == 12 and aadhar_clean.isdigit()
    
    def validate_pan(self, pan: str) -> bool:
        """Validate PAN card format (10 alphanumeric characters)."""
        pan_pattern = r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$'
        return bool(re.match(pan_pattern, pan.upper()))

    @staticmethod
    def calculate_stamp_duty(value: float) -> float:
        """
        Calculate stamp duty based on property valuation slabs (Indian Standards).

        Slabs:
            < ₹20,00,000          → 2%
            ₹20,00,000–45,00,000  → 3%
            > ₹45,00,000          → 5%
        """
        if value < 2_000_000:
            return value * 0.02
        elif value <= 4_500_000:
            return value * 0.03
        else:
            return value * 0.05

    @staticmethod
    def stamp_duty_rate(value: float) -> str:
        """Return the applicable stamp duty rate label for a given valuation."""
        if value < 2_000_000:
            return "2% (below ₹20,00,000)"
        elif value <= 4_500_000:
            return "3% (₹20,00,000–₹45,00,000)"
        else:
            return "5% (above ₹45,00,000)"

    def validate_aadhar_uniqueness(self, owner: str, aadhar: str) -> None:
        """Validate Aadhar uniqueness immediately upon entry.
        
        Raises:
            ValueError: If Aadhar is already used by someone else or owner has different Aadhar registered
        """
        aadhar_clean = aadhar.replace(" ", "").replace("-", "")

        # Check if this Aadhar is already used by someone else
        if aadhar_clean in self.aadhar_to_owner:
            owner_key = self.aadhar_to_owner[aadhar_clean]
            existing_owner = self.owner_registry.get(owner_key)
            if existing_owner is not None:
                raise ValueError(
                    f"Aadhar number {aadhar_clean} is already registered to '{existing_owner.name}'. "
                    f"Each Aadhar must be unique."
                )
    
    def validate_pan_uniqueness(self, owner: str, pan: str) -> None:
        """Validate PAN uniqueness immediately upon entry.
        
        Raises:
            ValueError: If PAN is already used by someone else or owner has different PAN registered
        """
        pan_clean = pan.upper()

        # Check if this PAN is already used by someone else
        if pan_clean in self.pan_to_owner:
            owner_key = self.pan_to_owner[pan_clean]
            existing_owner = self.owner_registry.get(owner_key)
            if existing_owner is not None:
                raise ValueError(
                    f"PAN number {pan_clean} is already registered to '{existing_owner.name}'. "
                    f"Each PAN must be unique."
                )
    
    def validate_survey_uniqueness(self, survey_no: str, property_key: str = None) -> None:
        """Validate Survey Number uniqueness immediately upon entry.
        
        Args:
            survey_no: Survey number to validate
            property_key: Current property key (for updates, not used in initial registration)
        
        Raises:
            ValueError: If survey number is already registered to another property
        """
        survey_clean = survey_no.strip()
        
        if survey_clean in self.survey_to_property:
            existing_property = self.survey_to_property[survey_clean]
            # Allow if it's the same property (shouldn't happen in add, but good for future)
            if property_key is None or existing_property != property_key:
                raise ValueError(
                    f"Survey number '{survey_clean}' is already registered to property '{existing_property}'. "
                    f"Each survey number must be unique."
                )
    
    def register_or_validate_owner(self, owner: Owner) -> Owner:
        """Register a new owner or validate an existing owner's identity."""
        aadhar_clean = owner.aadhar
        pan_clean = owner.pan

        owner_key_from_aadhar = self.aadhar_to_owner.get(aadhar_clean, "")
        owner_key_from_pan = self.pan_to_owner.get(pan_clean, "")

        if owner_key_from_aadhar and owner_key_from_pan and owner_key_from_aadhar != owner_key_from_pan:
            aadhar_owner = self.owner_registry[owner_key_from_aadhar]
            pan_owner = self.owner_registry[owner_key_from_pan]
            raise ValueError(
                "Identity mismatch: provided Aadhar and PAN belong to different registered owners "
                f"('{aadhar_owner.name}' and '{pan_owner.name}')."
            )

        existing_owner_key = owner_key_from_aadhar or owner_key_from_pan
        if existing_owner_key:
            reg_owner = self.owner_registry[existing_owner_key]
            if reg_owner.aadhar != aadhar_clean:
                raise ValueError(
                    f"Identity mismatch: Owner '{reg_owner.name}' is already registered "
                    f"with Aadhar {reg_owner.aadhar}, but provided {aadhar_clean}. "
                    "Same person cannot have multiple Aadhar numbers."
                )
            if reg_owner.pan != pan_clean:
                raise ValueError(
                    f"Identity mismatch: Owner '{reg_owner.name}' is already registered "
                    f"with PAN {reg_owner.pan}, but provided {pan_clean}. "
                    "Same person cannot have multiple PAN numbers."
                )
            return reg_owner

        self.owner_registry[owner.customer_key] = owner
        self.aadhar_to_owner[aadhar_clean] = owner.customer_key
        self.pan_to_owner[pan_clean] = owner.customer_key
        self.customer_key_to_owner[owner.customer_key] = owner.customer_key
        print(f"✓ Identity registered: {owner.name}")
        print(f"  Customer Key: {owner.customer_key}")
        print(f"  Aadhar: {aadhar_clean} | PAN: {pan_clean}")
        return owner
    
    def get_owner_by_customer_key(self, customer_key: str) -> Optional[Dict[str, Any]]:
        """Get owner information by customer key.
        
        Args:
            customer_key: The unique customer key
        
        Returns:
            Dictionary with owner information or None if not found
        """
        if customer_key in self.customer_key_to_owner:
            owner_key = self.customer_key_to_owner[customer_key]
            owner = self.owner_registry.get(owner_key)
            if owner is not None:
                return owner.to_dict()
        return None
    
    def add_property(self, property_key: str, owner: Owner, address: str, 
                     pincode: str, value: float, survey_no: str, rtc_no: str = "",
                     village: str = "", taluk: str = "", 
                     district: str = "", state: str = "",
                     land_area: str = "", land_type: str = "",
                     description: str = "", 
                     additional_info: Dict[str, Any] = None) -> Block:
        """
        Add a new property to the ledger (Indian Standards).
        
        Args:
            property_key: Unique identifier for the property
            owner: Current owner's name
            address: Property address
            pincode: Postal pincode
            value: Property value in INR
            aadhar_no: Owner's 12-digit Aadhar number
            pan_no: Owner's PAN card number
            survey_no: Survey/Map number of the property
            rtc_no: RTC (Record of Rights, Tenancy and Crops) number
            village: Village name
            taluk: Taluk/Tehsil name
            district: District name
            state: State name
            land_area: Area of land (e.g., "2 acres", "500 sq ft")
            land_type: Type of land (agricultural, residential, commercial)
            description: Optional description
            additional_info: Any additional property details
        
        Returns:
            The newly created block
        """
        if property_key in self.property_index:
            raise ValueError(f"Property with key '{property_key}' already exists. "
                           "Use transfer_property() for ownership changes.")
        
        if not self.validate_aadhar(owner.aadhar):
            raise ValueError("Invalid Aadhar number. Must be 12 digits.")
        if not self.validate_pan(owner.pan):
            raise ValueError("Invalid PAN number. Must be in format: ABCDE1234F")
        # Validate all constraints before mutating any registry
        self.validate_survey_uniqueness(survey_no)
        registered_owner = self.register_or_validate_owner(owner)
        data = {
            "type": "registration",
            "status": self.STATUS_VALID,
            "transaction_id": self._next_transaction_id(),
            "owner": registered_owner.name,
            "customer_key": registered_owner.customer_key,
            "aadhar_no": registered_owner.aadhar,
            "pan_no": registered_owner.pan,
            "address": address.strip().upper(),
            "pincode": pincode.strip().upper(),
            "value": value,
            "survey_no": survey_no.strip().upper(),
            "rtc_no": rtc_no.strip().upper(),
            "location": {
                "village": village.strip().upper(),
                "taluk": taluk.strip().upper(),
                "district": district.strip().upper(),
                "state": state.strip().upper()
            },
            "land_details": {
                "area": land_area.strip().upper(),
                "type": land_type.strip().upper()
            },
            "description": description.strip().upper(),
            "additional_info": additional_info or {}
        }
        new_block = Block(
            index=len(self.chain),
            timestamp=datetime.now().isoformat(),
            data=data,
            previous_hash=self.get_latest_block().hash,
            property_key=property_key
        )
        self.chain.append(new_block)
        self.property_index[property_key] = [new_block.index]
        self.survey_to_property[survey_no.strip()] = property_key
        print(f"✓ Property '{property_key}' registered successfully!")
        return new_block
    
    def transfer_property(self, property_key: str, new_owner: Owner,
                          transfer_value: float = None, 
                          transfer_reason: str = "sale",
                          stamp_duty_paid: float = None,
                          registration_fee: float = None,
                          additional_info: Dict[str, Any] = None) -> Block:
        """
        Transfer property ownership (inheritance or sale).
        
        Args:
            property_key: The property's unique identifier
            new_owner: Owner object for the new owner (create with Owner(name, aadhar, pan))
            transfer_value: Value at transfer in INR (optional)
            transfer_reason: Reason for transfer (e.g., 'sale', 'inheritance', 'gift')
            stamp_duty_paid: Stamp duty amount paid
            registration_fee: Registration fee paid
            additional_info: Additional transfer details
        
        Returns:
            The newly created transfer block
        """
        if property_key not in self.property_index:
            raise ValueError(f"Property with key '{property_key}' not found.")
        
        # Validate new owner's Aadhar
        if not self.validate_aadhar(new_owner.aadhar):
            raise ValueError("Invalid Aadhar number for new owner. Must be 12 digits.")
        
        # Validate new owner's PAN
        if not self.validate_pan(new_owner.pan):
            raise ValueError("Invalid PAN number for new owner. Must be in format: ABCDE1234F")
        
        # Register or validate new owner (ensures Aadhar and PAN uniqueness)
        registered_owner = self.register_or_validate_owner(new_owner)
        
        # Get current property state
        current_state = self.get_property_current_state(property_key)
        previous_owner = current_state["owner"]
        previous_owner_key = current_state.get("customer_key", "")
        
        # Prevent self-transfer: owner cannot sell property to themselves
        if previous_owner_key and previous_owner_key == registered_owner.customer_key:
            raise ValueError(
                f"Cannot transfer property to the same owner. "
                f"'{previous_owner}' already owns this property."
            )
        
        # Determine actual transfer value (explicit None check to allow 0.0)
        actual_transfer_value = transfer_value if transfer_value is not None else current_state.get("value")
        
        # Calculate default stamp duty (slab-based) and registration fee (5%) if not provided
        if stamp_duty_paid is None:
            stamp_duty_paid = self.calculate_stamp_duty(actual_transfer_value)
        if registration_fee is None:
            registration_fee = actual_transfer_value * 0.05
        
        data = {
            "type": "transfer",
            "status": self.STATUS_VALID,
            "transaction_id": self._next_transaction_id(),
            "transfer_reason": transfer_reason.strip().upper(),
            "previous_owner": previous_owner,
            "previous_owner_aadhar": current_state.get("aadhar_no", ""),
            "previous_customer_key": current_state.get("customer_key", ""),
            "new_owner": registered_owner.name,
            "new_owner_customer_key": registered_owner.customer_key,
            "new_owner_aadhar": registered_owner.aadhar,
            "new_owner_pan": registered_owner.pan,
            "transfer_value": actual_transfer_value,
            "stamp_duty_paid": stamp_duty_paid,
            "registration_fee": registration_fee,
            "transaction_cost": stamp_duty_paid + registration_fee,
            "total_value": actual_transfer_value + stamp_duty_paid + registration_fee,
            "address": current_state["address"],
            "pincode": current_state.get("pincode", ""),
            "location": current_state.get("location", {}),
            "survey_no": current_state.get("survey_no", ""),
            "rtc_no": current_state.get("rtc_no", ""),
            "stamp_duty_paid": stamp_duty_paid,
            "registration_fee": registration_fee,
            "additional_info": additional_info or {}
        }
        
        new_block = Block(
            index=len(self.chain),
            timestamp=datetime.now().isoformat(),
            data=data,
            previous_hash=self.get_latest_block().hash,
            property_key=property_key
        )
        
        self.chain.append(new_block)
        self.property_index[property_key].append(new_block.index)
        
        print(f"✓ Property '{property_key}' transferred from '{previous_owner}' "
              f"to '{registered_owner.name}' ({transfer_reason})")
        return new_block
    
    def inherit_property(self, property_key: str, heir: Owner,
                         relationship: str = "", 
                         legal_heir_certificate_no: str = "",
                         additional_info: Dict[str, Any] = None) -> Block:
        """
        Transfer property through inheritance.
        
        Args:
            property_key: The property's unique identifier
            heir: Owner object for the heir (create with Owner(name, aadhar, pan))
            relationship: Relationship to previous owner
            legal_heir_certificate_no: Legal heir certificate number
            additional_info: Additional inheritance details
        
        Returns:
            The newly created inheritance block
        """
        info = additional_info or {}
        info["relationship"] = relationship.strip().upper()
        info["legal_heir_certificate_no"] = legal_heir_certificate_no.strip().upper()
        
        return self.transfer_property(
            property_key=property_key,
            new_owner=heir,
            transfer_reason="inheritance",
            additional_info=info
        )

    def create_correction_transaction(
        self,
        property_key: str,
        original_transaction_id: str,
        corrected_data: Dict[str, Any],
        correction_request_id: str,
        approved_by_authority: str,
    ) -> Block:
        """Append a correction block without mutating historical transactions."""
        if property_key not in self.property_index:
            raise ValueError(f"Property with key '{property_key}' not found.")

        original_tx = self.get_property_transaction_by_id(property_key, original_transaction_id)
        original_status = original_tx.get("status", self.STATUS_VALID)
        if original_status != self.STATUS_VALID:
            raise ValueError(f"Transaction '{original_transaction_id}' is not in VALID state.")

        snapshot = self.get_property_current_state(property_key)

        owner_name = str(corrected_data.get("owner_name", snapshot.get("owner", ""))).strip().upper()
        aadhar_no = str(corrected_data.get("aadhar_no", snapshot.get("aadhar_no", ""))).replace(" ", "").replace("-", "")
        pan_no = str(corrected_data.get("pan_no", snapshot.get("pan_no", ""))).strip().upper()

        if not owner_name:
            raise ValueError("Owner name cannot be empty.")
        if not self.validate_aadhar(aadhar_no):
            raise ValueError("Invalid Aadhar number. Must be 12 digits.")
        if not self.validate_pan(pan_no):
            raise ValueError("Invalid PAN number. Must be in format: ABCDE1234F")

        same_identity_name_correction = (
            aadhar_no == snapshot.get("aadhar_no", "")
            and pan_no == snapshot.get("pan_no", "")
            and owner_name != snapshot.get("owner", "")
        )

        if same_identity_name_correction:
            corrected_owner = Owner.from_dict(
                {
                    "name": owner_name,
                    "aadhar": aadhar_no,
                    "pan": pan_no,
                    "customer_key": snapshot.get("customer_key", "") or Owner(owner_name, aadhar_no, pan_no).customer_key,
                }
            )
            self.owner_registry[corrected_owner.customer_key] = corrected_owner
            self.aadhar_to_owner[aadhar_no] = corrected_owner.customer_key
            self.pan_to_owner[pan_no] = corrected_owner.customer_key
            self.customer_key_to_owner[corrected_owner.customer_key] = corrected_owner.customer_key
        else:
            corrected_owner = Owner(owner_name, aadhar_no, pan_no)
            corrected_owner = self.register_or_validate_owner(corrected_owner)

        corrected_value = corrected_data.get("value", snapshot.get("value", 0))
        corrected_address = str(corrected_data.get("address", snapshot.get("address", ""))).strip().upper()
        corrected_pincode = str(corrected_data.get("pincode", snapshot.get("pincode", ""))).strip().upper()

        payload = {
            "owner_name": corrected_owner.name,
            "aadhar_no": corrected_owner.aadhar,
            "pan_no": corrected_owner.pan,
            "address": corrected_address,
            "pincode": corrected_pincode,
            "value": corrected_value,
        }

        data = {
            "type": "correction",
            "status": self.STATUS_VALID,
            "transaction_id": self._next_transaction_id(),
            "previous_transaction_id": original_transaction_id.strip().upper(),
            "correction_request_id": correction_request_id,
            "approved_by_authority": approved_by_authority,
            "previous_snapshot": {
                "owner": snapshot.get("owner", ""),
                "aadhar_no": snapshot.get("aadhar_no", ""),
                "pan_no": snapshot.get("pan_no", ""),
                "address": snapshot.get("address", ""),
                "pincode": snapshot.get("pincode", ""),
                "value": snapshot.get("value", 0),
            },
            "corrected_fields": payload,
            "new_owner": corrected_owner.name,
            "new_owner_customer_key": corrected_owner.customer_key,
            "new_owner_aadhar": corrected_owner.aadhar,
            "new_owner_pan": corrected_owner.pan,
            "address": corrected_address,
            "pincode": corrected_pincode,
            "value": corrected_value,
            "location": snapshot.get("location", {}),
            "survey_no": snapshot.get("survey_no", ""),
            "rtc_no": snapshot.get("rtc_no", ""),
            "land_details": snapshot.get("land_details", {}),
            "description": snapshot.get("description", ""),
            "additional_info": {"reason": "BLOCKCHAIN_CORRECTION"},
        }

        new_block = Block(
            index=len(self.chain),
            timestamp=datetime.now().isoformat(),
            data=data,
            previous_hash=self.get_latest_block().hash,
            property_key=property_key,
        )
        self.chain.append(new_block)
        self.property_index[property_key].append(new_block.index)
        return new_block
    
    def get_property_history(self, property_key: str) -> List[Dict[str, Any]]:
        """
        Retrieve complete history of a property by its key.
        
        Args:
            property_key: The property's unique identifier
        
        Returns:
            List of all blocks related to this property
        """
        if property_key not in self.property_index:
            raise ValueError(f"Property with key '{property_key}' not found.")
        
        block_indices = self.property_index[property_key]
        corrected_targets = self._property_correction_targets(property_key)
        history: List[Dict[str, Any]] = []

        for idx in block_indices:
            record = self.chain[idx].to_dict()
            tx_id = record["data"].get("transaction_id", self._tx_id_for_index(record["index"]))
            status_value = record["data"].get("status", self.STATUS_VALID)
            if status_value == self.STATUS_VALID and tx_id in corrected_targets:
                status_value = self.STATUS_CORRECTED

            raw_type = record["data"].get("type", "unknown")
            display_type = raw_type
            display_label = raw_type.upper()
            display_data = record["data"]

            if raw_type == "correction":
                display_type = "registration"
                display_label = "REGISTERED"
                display_data = {
                    **record["data"],
                    "owner": record["data"].get("new_owner", ""),
                    "customer_key": record["data"].get("new_owner_customer_key", ""),
                    "aadhar_no": record["data"].get("new_owner_aadhar", ""),
                    "pan_no": record["data"].get("new_owner_pan", ""),
                    "address": record["data"].get("address", ""),
                    "pincode": record["data"].get("pincode", ""),
                    "value": record["data"].get("value", 0),
                    "survey_no": record["data"].get("survey_no", ""),
                }

            record["transaction_id"] = tx_id
            record["event_type"] = display_label
            record["status"] = status_value
            record["display_type"] = display_type
            record["display_data"] = display_data
            history.append(record)
        return history
    
    def get_property_current_state(self, property_key: str) -> Dict[str, Any]:
        """
        Get the current state of a property.
        
        Args:
            property_key: The property's unique identifier
        
        Returns:
            Current property details including owner, address, value
        """
        if property_key not in self.property_index:
            raise ValueError(f"Property with key '{property_key}' not found.")
        
        # Get the latest block for this property
        latest_index = self.property_index[property_key][-1]
        latest_block = self.chain[latest_index]
        
        # Build current state from history
        history = self.get_property_history(property_key)
        
        # Start with registration data
        registration = history[0]["data"]
        current_state = {
            "property_key": property_key,
            "owner": registration["owner"],
            "customer_key": registration.get("customer_key", ""),
            "aadhar_no": registration.get("aadhar_no", ""),
            "pan_no": registration.get("pan_no", ""),
            "address": registration["address"],
            "pincode": registration.get("pincode", ""),
            "value": registration["value"],
            "survey_no": registration.get("survey_no", ""),
            "rtc_no": registration.get("rtc_no", ""),
            "location": registration.get("location", {}),
            "land_details": registration.get("land_details", {}),
            "description": registration.get("description", ""),
            "registered_on": history[0]["timestamp"],
            "last_updated": latest_block.timestamp,
            "total_transfers": len(history) - 1
        }
        
        # Update with latest transfer/correction info if any
        if len(history) > 1:
            latest_data = history[-1]["data"]
            latest_type = latest_data.get("type", "")
            if latest_type in ("transfer", "inheritance"):
                current_state["owner"] = latest_data["new_owner"]
                current_state["customer_key"] = latest_data.get("new_owner_customer_key", "")
                current_state["aadhar_no"] = latest_data.get("new_owner_aadhar", "")
                current_state["pan_no"] = latest_data.get("new_owner_pan", "")
                # Valuation = transfer price + stamp duty + registration fee
                if latest_data.get("total_value") is not None:
                    current_state["value"] = latest_data["total_value"]
                elif latest_data.get("transfer_value") is not None:
                    current_state["value"] = latest_data["transfer_value"]
            elif latest_type == "correction":
                current_state["owner"] = latest_data.get("new_owner", current_state["owner"])
                current_state["customer_key"] = latest_data.get("new_owner_customer_key", current_state["customer_key"])
                current_state["aadhar_no"] = latest_data.get("new_owner_aadhar", current_state["aadhar_no"])
                current_state["pan_no"] = latest_data.get("new_owner_pan", current_state["pan_no"])
                current_state["address"] = latest_data.get("address", current_state["address"])
                current_state["pincode"] = latest_data.get("pincode", current_state["pincode"])
                current_state["value"] = latest_data.get("value", current_state["value"])

        current_state["status"] = history[-1].get("status", self.STATUS_VALID)
        current_state["last_transaction_id"] = history[-1].get("transaction_id", "")
        
        return current_state
    
    def get_block_by_key(self, property_key: str, 
                         block_index: int = None) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific block by property key.
        
        Args:
            property_key: The property's unique identifier
            block_index: Optional specific block index (defaults to latest)
        
        Returns:
            Block data as dictionary
        """
        if property_key not in self.property_index:
            return None
        
        indices = self.property_index[property_key]
        
        if block_index is not None:
            if block_index not in indices:
                raise ValueError(f"Block index {block_index} not found for property '{property_key}'")
            return self.chain[block_index].to_dict()
        
        # Return latest block for this property
        return self.chain[indices[-1]].to_dict()
    
    def search_by_owner(self, owner: str) -> List[Dict[str, Any]]:
        """
        Find all properties currently owned by a specific owner.
        
        Args:
            owner: Owner's name/ID to search for
        
        Returns:
            List of property current states
        """
        results = []
        for property_key in self.property_index:
            try:
                state = self.get_property_current_state(property_key)
                if state["owner"].lower() == owner.lower():
                    results.append(state)
            except Exception:
                continue
        return results

    @staticmethod
    def _normalized_identity(
        customer_key: str,
        aadhar_no: str = "",
        pan_no: str = "",
        owner_name: str = "",
    ) -> Dict[str, str]:
        """Normalize a citizen identity for property ownership matching."""
        return {
            "customer_key": customer_key.strip().upper(),
            "aadhar_no": aadhar_no.replace(" ", "").replace("-", ""),
            "pan_no": pan_no.strip().upper(),
            "owner_name": owner_name.strip().upper(),
        }

    @staticmethod
    def _matches_identity(
        identity: Dict[str, str],
        record: Dict[str, Any],
        mappings: Dict[str, str],
    ) -> bool:
        """Return True when any provided identity field matches the record."""
        for identity_field, record_field in mappings.items():
            identity_value = identity.get(identity_field, "")
            if not identity_value:
                continue

            record_value = str(record.get(record_field, "")).strip().upper()
            if identity_field == "aadhar_no":
                record_value = record_value.replace(" ", "").replace("-", "")

            if record_value == identity_value:
                return True
        return False

    @staticmethod
    def _owner_snapshot_from_record(record: Dict[str, Any]) -> Dict[str, str]:
        """Extract the resulting owner identity after a history record is applied."""
        record_type = record.get("type", "")

        if record_type == "registration":
            return {
                "owner_name": str(record.get("owner", "")).strip().upper(),
                "customer_key": str(record.get("customer_key", "")).strip().upper(),
                "aadhar_no": str(record.get("aadhar_no", "")).replace(" ", "").replace("-", ""),
                "pan_no": str(record.get("pan_no", "")).strip().upper(),
            }

        return {
            "owner_name": str(record.get("new_owner", record.get("owner", ""))).strip().upper(),
            "customer_key": str(record.get("new_owner_customer_key", record.get("customer_key", ""))).strip().upper(),
            "aadhar_no": str(record.get("new_owner_aadhar", record.get("aadhar_no", ""))).replace(" ", "").replace("-", ""),
            "pan_no": str(record.get("new_owner_pan", record.get("pan_no", ""))).strip().upper(),
        }

    def get_properties_by_customer_key(
        self,
        customer_key: str,
        aadhar_no: str = "",
        pan_no: str = "",
        owner_name: str = "",
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Find all properties linked to a customer key — both currently owned
        and previously owned (transferred away).

        Args:
            customer_key: The CUST-XXXXXXXX identifier

        Returns:
            Dict with keys 'current' and 'past', each a list of property dicts.
            Past entries include 'transferred_on' and 'transferred_to' fields.
        """
        current: List[Dict[str, Any]] = []
        past: List[Dict[str, Any]] = []
        identity = self._normalized_identity(customer_key, aadhar_no, pan_no, owner_name)

        for property_key in self.property_index:
            try:
                state = self.get_property_current_state(property_key)
                history = self.get_property_history(property_key)
            except Exception:
                continue

            # Check if citizen currently owns this property
            if self._matches_identity(
                identity,
                state,
                {
                    "customer_key": "customer_key",
                    "aadhar_no": "aadhar_no",
                    "pan_no": "pan_no",
                    "owner_name": "owner",
                },
            ):
                current.append(state)
                continue

            ownership_started_on = ""
            latest_past_entry: Optional[Dict[str, Any]] = None

            for record in history:
                owner_snapshot = self._owner_snapshot_from_record(record["data"])
                if not self._matches_identity(
                    identity,
                    owner_snapshot,
                    {
                        "customer_key": "customer_key",
                        "aadhar_no": "aadhar_no",
                        "pan_no": "pan_no",
                        "owner_name": "owner_name",
                    },
                ):
                    if ownership_started_on:
                        transfer_reason = str(record["data"].get("transfer_reason", "")).strip().upper()
                        latest_past_entry = {
                            **state,
                            "owned_from": ownership_started_on,
                            "transferred_on": record["timestamp"],
                            "transferred_to": owner_snapshot.get("owner_name", ""),
                            "transfer_reason": transfer_reason or record["data"].get("type", "").upper(),
                        }
                        ownership_started_on = ""
                    continue

                if not ownership_started_on:
                    ownership_started_on = record["timestamp"]

            if latest_past_entry is not None:
                past.append(latest_past_entry)

        return {"current": current, "past": past}

    def search_properties(self, query: str, field: str = "all") -> List[Dict[str, Any]]:
        """
        Fuzzy search across properties by multiple fields.

        Args:
            query: Search term (partial match supported)
            field: One of 'all', 'owner', 'property_key', 'customer_key',
                   'aadhar', 'pan', 'address', 'survey_no', 'district'

        Returns:
            List of matching property current states, scored by relevance
        """
        query_lower = query.strip().lower()
        if not query_lower:
            return []

        scored_results: List[tuple] = []

        for property_key in self.property_index:
            try:
                state = self.get_property_current_state(property_key)
            except Exception:
                continue

            score = self._score_property(state, query_lower, field)
            if score > 0:
                scored_results.append((score, state))

        scored_results.sort(key=lambda pair: pair[0], reverse=True)
        return [state for _, state in scored_results]

    def _score_property(self, state: Dict[str, Any], query: str, field: str) -> int:
        """Return a relevance score (0 = no match) for a property against a query."""
        score = 0

        searchable_fields = {
            "owner":        state.get("owner", ""),
            "property_key": state.get("property_key", ""),
            "customer_key": state.get("customer_key", ""),
            "aadhar":       state.get("aadhar_no", ""),
            "pan":          state.get("pan_no", ""),
            "address":      state.get("address", ""),
            "survey_no":    state.get("survey_no", ""),
            "district":     (state.get("location") or {}).get("district", ""),
        }

        # Determine which fields to search
        if field == "all":
            fields_to_check = searchable_fields
        elif field in searchable_fields:
            fields_to_check = {field: searchable_fields[field]}
        else:
            fields_to_check = searchable_fields

        # Weight: exact match > starts-with > contains
        EXACT_WEIGHT = 100
        STARTS_WEIGHT = 50
        CONTAINS_WEIGHT = 20
        # Bonus for primary identifiers
        PRIMARY_FIELDS = {"property_key", "customer_key", "aadhar", "pan", "survey_no"}

        for field_name, value in fields_to_check.items():
            value_lower = value.lower()
            if not value_lower:
                continue

            multiplier = 2 if field_name in PRIMARY_FIELDS else 1

            if value_lower == query:
                score += EXACT_WEIGHT * multiplier
            elif value_lower.startswith(query):
                score += STARTS_WEIGHT * multiplier
            elif query in value_lower:
                score += CONTAINS_WEIGHT * multiplier
            else:
                # Token-level fuzzy: check if query matches any word
                tokens = value_lower.split()
                for token in tokens:
                    if token.startswith(query) or query.startswith(token):
                        score += 10 * multiplier
                        break

        return score
    
    def is_chain_valid(self) -> bool:
        """Verify the integrity of the blockchain."""
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i - 1]
            
            # Check if current hash is correct
            if current_block.hash != current_block.calculate_hash():
                print(f"✗ Invalid hash at block {i}")
                return False
            
            # Check if previous hash reference is correct
            if current_block.previous_hash != previous_block.hash:
                print(f"✗ Invalid chain link at block {i}")
                return False
        
        print("✓ Blockchain is valid")
        return True
    
    def get_analytics_data(self) -> Dict[str, Any]:
        """
        Aggregate analytics data from the blockchain for charting and mapping.

        Returns:
            Dict with keys:
                'timeline': list of {month, registrations, transfers, inheritances}
                'locations': list of {property_key, district, state, value, owner}
                'heatmap': list of {district, count, total_value, avg_value}
        """
        from collections import defaultdict

        # Timeline: group transactions by month
        monthly: Dict[str, Dict[str, int]] = defaultdict(lambda: {"registrations": 0, "transfers": 0, "inheritances": 0})

        for block in self.chain:
            if block.property_key == "GENESIS":
                continue
            month = block.timestamp[:7]  # "YYYY-MM"
            tx_type = block.data.get("type", "")
            if tx_type == "registration":
                monthly[month]["registrations"] += 1
            elif tx_type == "transfer":
                reason = block.data.get("transfer_reason", "SALE").upper()
                if reason == "INHERITANCE":
                    monthly[month]["inheritances"] += 1
                else:
                    monthly[month]["transfers"] += 1

        # Sort by month
        sorted_months = sorted(monthly.keys())
        timeline = [
            {"month": m, **monthly[m]} for m in sorted_months
        ]

        # Locations and heatmap
        district_data: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"count": 0, "total_value": 0.0})
        locations = []

        for property_key in self.property_index:
            try:
                state = self.get_property_current_state(property_key)
            except Exception:
                continue

            loc = state.get("location", {})
            district = loc.get("district", "").strip() or "UNKNOWN"
            state_name = loc.get("state", "").strip() or ""
            village = loc.get("village", "").strip() or ""
            taluk = loc.get("taluk", "").strip() or ""
            value = state.get("value", 0)

            locations.append({
                "property_key": property_key,
                "village": village,
                "taluk": taluk,
                "district": district,
                "state": state_name,
                "value": value,
                "owner": state.get("owner", ""),
                "address": state.get("address", ""),
                "survey_no": state.get("survey_no", ""),
            })

            district_data[district]["count"] += 1
            district_data[district]["total_value"] += value

        heatmap = sorted([
            {
                "district": d,
                "count": data["count"],
                "total_value": data["total_value"],
                "avg_value": data["total_value"] / data["count"] if data["count"] > 0 else 0,
            }
            for d, data in district_data.items()
        ], key=lambda x: x["total_value"], reverse=True)

        return {
            "timeline": timeline,
            "locations": locations,
            "heatmap": heatmap,
        }

    def get_all_properties(self) -> List[Dict[str, Any]]:
        """Get current state of all registered properties."""
        return [self.get_property_current_state(key)
                for key in self.property_index]
    
    def print_chain(self) -> None:
        """Print the entire blockchain."""
        print("\n" + "="*60)
        print("PROPERTY LEDGER BLOCKCHAIN")
        print("="*60)
        for block in self.chain:
            print(f"\nBlock #{block.index}")
            print(f"  Timestamp: {block.timestamp}")
            print(f"  Property Key: {block.property_key}")
            print(f"  Data: {json.dumps(block.data, indent=4)}")
            print(f"  Previous Hash: {block.previous_hash[:20]}...")
            print(f"  Hash: {block.hash[:20]}...")
            print("-"*60)
    
    def _init_db(self) -> None:
        """Initialize the SQLite database and create the BLOB storage table if needed."""
        conn = sqlite3.connect(self.DB_FILE)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS blockchain_store (
                    id       INTEGER PRIMARY KEY,
                    data     BLOB    NOT NULL,
                    saved_at TEXT    NOT NULL
                )
            """)
            conn.commit()
        finally:
            conn.close()

    def _encrypt_data(self, data: str) -> bytes:
        """Encrypt data using XOR with a SHA-256 key; returns raw bytes for BLOB storage."""
        key = hashlib.sha256(b"pawperty_blockchain_key").digest()
        data_bytes = data.encode('utf-8')
        encrypted_bytes = bytearray(
            byte ^ key[i % len(key)] for i, byte in enumerate(data_bytes)
        )
        return bytes(encrypted_bytes)

    def _decrypt_data(self, encrypted_data: bytes) -> str:
        """Decrypt raw bytes that were encrypted with _encrypt_data."""
        key = hashlib.sha256(b"pawperty_blockchain_key").digest()
        decrypted_bytes = bytearray(
            byte ^ key[i % len(key)] for i, byte in enumerate(encrypted_data)
        )
        return decrypted_bytes.decode('utf-8')
    
    def _save_blockchain(self) -> bool:
        """Save blockchain as an encrypted snapshot in the SQLite database."""
        try:
            blockchain_data = {
                "chain": [block.to_dict() for block in self.chain],
                "property_index": self.property_index,
                "owner_registry": {customer_key: owner.to_dict()
                                   for customer_key, owner in self.owner_registry.items()},
                "aadhar_to_owner": self.aadhar_to_owner,
                "pan_to_owner": self.pan_to_owner,
                "customer_key_to_owner": self.customer_key_to_owner,
                "survey_to_property": self.survey_to_property,
                "saved_at": datetime.now().isoformat()
            }

            json_data = json.dumps(blockchain_data, indent=2)
            encrypted_blob: bytes = self._encrypt_data(json_data)
            saved_at = datetime.now().isoformat()

            conn = sqlite3.connect(self.DB_FILE)
            try:
                # id=1 keeps a single always-current snapshot (REPLACE overwrites it)
                conn.execute(
                    "INSERT OR REPLACE INTO blockchain_store (id, data, saved_at) VALUES (1, ?, ?)",
                    (sqlite3.Binary(encrypted_blob), saved_at)
                )
                conn.commit()
            finally:
                conn.close()

            print(f"✓ Blockchain saved to database: {self.DB_FILE}")
            return True

        except Exception as e:
            print(f"✗ Error saving blockchain: {e}")
            return False
    
    def _load_blockchain(self) -> bool:
        """Load blockchain from the encrypted snapshot stored in the SQLite database."""
        try:
            if not os.path.exists(self.DB_FILE):
                return False

            conn = sqlite3.connect(self.DB_FILE)
            try:
                # Ensure the table exists in case the DB file was created externally
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS blockchain_store (
                        id      INTEGER PRIMARY KEY,
                        data    TEXT    NOT NULL,
                        saved_at TEXT   NOT NULL
                    )
                """)
                cursor = conn.execute("SELECT data FROM blockchain_store WHERE id = 1")
                row = cursor.fetchone()
            finally:
                conn.close()

            if row is None:
                return False

            # row[0] is a bytes/memoryview BLOB from SQLite
            blob: bytes = bytes(row[0])
            json_data = self._decrypt_data(blob)
            blockchain_data = json.loads(json_data)

            # Reconstruct chain
            self.chain = []
            for block_dict in blockchain_data["chain"]:
                block = Block(
                    index=block_dict["index"],
                    timestamp=block_dict["timestamp"],
                    data=block_dict["data"],
                    previous_hash=block_dict["previous_hash"],
                    property_key=block_dict["property_key"]
                )
                block.hash = block_dict["hash"]
                self.chain.append(block)

            self.property_index = blockchain_data["property_index"]

            raw_owner_registry = blockchain_data.get("owner_registry", {})
            self.owner_registry = {}
            self.aadhar_to_owner = {}
            self.pan_to_owner = {}
            self.customer_key_to_owner = {}

            for owner_dict in raw_owner_registry.values():
                owner = Owner.from_dict(owner_dict)
                self.owner_registry[owner.customer_key] = owner
                self.aadhar_to_owner[owner.aadhar] = owner.customer_key
                self.pan_to_owner[owner.pan] = owner.customer_key
                self.customer_key_to_owner[owner.customer_key] = owner.customer_key

            self.survey_to_property = blockchain_data.get("survey_to_property", {})

            if not self.is_chain_valid():
                print("✗ Warning: Loaded blockchain failed integrity validation")
                return False

            return True

        except Exception as e:
            print(f"✗ Error loading blockchain: {e}")
            return False
    
    def save_and_exit(self) -> None:
        """Save blockchain to encrypted storage before exiting."""
        self._save_blockchain()


# Interactive Menu System
def display_menu():
    """Display the main menu."""
    print("\n" + "="*60)
    print("       PROPERTY LEDGER BLOCKCHAIN (INDIAN STANDARDS)")
    print("="*60)
    print("1. Add New Property")
    print("2. Transfer Property (Sale)")
    print("3. Inherit Property")
    print("4. View Property by Key")
    print("5. View Property History")
    print("6. View Current Property State")
    print("7. Search Properties by Owner")
    print("8. View All Properties")
    print("9. Validate Blockchain")
    print("10. Print Full Blockchain")
    print("11. Save Blockchain")
    print("0. Exit")
    print("-"*60)


def get_input(prompt: str, required: bool = True) -> str:
    """Get user input, always stored as uppercase."""
    while True:
        value = input(prompt).strip()
        if value or not required:
            return value.upper()
        print("This field is required. Please enter a value.")


def get_float_input(prompt: str) -> float:
    """Get float input from user."""
    while True:
        try:
            return float(input(prompt).strip())
        except ValueError:
            print("Please enter a valid number.")


def get_validated_aadhar(ledger: PropertyBlockchain, owner: str, prompt: str) -> str:
    """Get Aadhar number with immediate format and uniqueness validation."""
    while True:
        aadhar = input(prompt).strip().upper()
        # Validate format first
        if not ledger.validate_aadhar(aadhar):
            print("✗ Invalid Aadhar number format. Must be 12 digits.")
            continue
        # Validate uniqueness
        try:
            ledger.validate_aadhar_uniqueness(owner, aadhar)
            return aadhar
        except ValueError as e:
            print(f"✗ Error: {e}")
            retry = input("  Try again? (y/n): ").strip().lower()
            if retry != 'y':
                raise


def get_validated_pan(ledger: PropertyBlockchain, owner: str, prompt: str) -> str:
    """Get PAN number with immediate format and uniqueness validation."""
    while True:
        pan = input(prompt).strip().upper()
        # Validate format first
        if not ledger.validate_pan(pan):
            print("✗ Invalid PAN number format. Must be in format: ABCDE1234F")
            continue
        # Validate uniqueness
        try:
            ledger.validate_pan_uniqueness(owner, pan)
            return pan
        except ValueError as e:
            print(f"✗ Error: {e}")
            retry = input("  Try again? (y/n): ").strip().lower()
            if retry != 'y':
                raise


def get_validated_survey(ledger: PropertyBlockchain, prompt: str) -> str:
    """Get Survey Number with immediate uniqueness validation."""
    while True:
        survey = input(prompt).strip().upper()
        if not survey:
            print("✗ Survey number is required.")
            continue
        # Validate uniqueness
        try:
            ledger.validate_survey_uniqueness(survey)
            return survey
        except ValueError as e:
            print(f"✗ Error: {e}")
            retry = input("  Try again? (y/n): ").strip().lower()
            if retry != 'y':
                raise


def add_property_menu(ledger: PropertyBlockchain):
    """Menu for adding a new property."""
    print("\n--- ADD NEW PROPERTY ---")
    print("Enter property details:\n")
    
    try:
        property_key = get_input("Property Key (unique identifier): ")
        owner_name = get_input("Owner Name: ")
        aadhar_no = get_validated_aadhar(ledger, owner_name, "Owner's Aadhar Number (12 digits): ")
        pan_no = get_validated_pan(ledger, owner_name, "Owner's PAN Number (e.g., ABCDE1234F): ")
        owner = Owner(owner_name, aadhar_no, pan_no)
        address = get_input("Property Address: ")
        pincode = get_input("Pincode: ")
        value = get_float_input("Property Value (in INR): ")
        survey_no = get_validated_survey(ledger, "Survey Number: ")
        rtc_no = get_input("RTC Number: ", required=False)
        print("\n-- Location Details --")
        village = get_input("Village: ", required=False)
        taluk = get_input("Taluk/Tehsil: ", required=False)
        district = get_input("District: ", required=False)
        state = get_input("State: ", required=False)
        print("\n-- Land Details --")
        land_area = get_input("Land Area (e.g., 2400 sq ft, 5 acres): ", required=False)
        land_type = get_input("Land Type (residential/commercial/agricultural): ", required=False)
        description = get_input("Description: ", required=False)
        ledger.add_property(
            property_key=property_key,
            owner=owner,
            address=address,
            pincode=pincode,
            value=value,
            survey_no=survey_no,
            rtc_no=rtc_no,
            village=village,
            taluk=taluk,
            district=district,
            state=state,
            land_area=land_area,
            land_type=land_type,
            description=description
        )
    except ValueError as e:
        print(f"\n✗ Error: {e}")


def transfer_property_menu(ledger: PropertyBlockchain):
    """Menu for transferring property."""
    print("\n--- TRANSFER PROPERTY (SALE) ---")
    print("Enter transfer details:\n")
    
    property_key = get_input("Property Key: ")
    
    # Check if property exists
    if property_key not in ledger.property_index:
        print(f"\n✗ Error: Property '{property_key}' not found.")
        return
    
    # Show current owner
    current_state = ledger.get_property_current_state(property_key)
    print(f"\nCurrent Owner: {current_state['owner']}")
    print(f"Current Value: ₹{current_state['value']:,.2f}\n")
    
    try:
        new_owner_name = get_input("New Owner Name: ")
        
        # Immediate validation for new owner's Aadhar
        new_owner_aadhar = get_validated_aadhar(ledger, new_owner_name, "New Owner's Aadhar Number (12 digits): ")
        
        # Immediate validation for new owner's PAN
        new_owner_pan = get_validated_pan(ledger, new_owner_name, "New Owner's PAN Number: ")
        
        # Create Owner object before the transaction
        new_owner = Owner(new_owner_name, new_owner_aadhar, new_owner_pan)
        
        transfer_value = get_float_input("Transfer Value (in INR): ")
        
        # Calculate and show default stamp duty (slab-based) and registration fee (5%)
        default_stamp_duty = ledger.calculate_stamp_duty(transfer_value)
        default_registration_fee = transfer_value * 0.05
        rate_label = ledger.stamp_duty_rate(transfer_value)

        print(f"\nDefault Stamp Duty ({rate_label}): ₹{default_stamp_duty:,.2f}")
        custom_stamp = input("  Enter custom amount or press Enter to use default: ").strip()
        stamp_duty_paid = float(custom_stamp) if custom_stamp else None
        
        print(f"\nDefault Registration Fee (5%): ₹{default_registration_fee:,.2f}")
        custom_reg = input("  Enter custom amount or press Enter to use default: ").strip()
        registration_fee = float(custom_reg) if custom_reg else None
        
        ledger.transfer_property(
            property_key=property_key,
            new_owner=new_owner,
            transfer_value=transfer_value,
            transfer_reason="sale",
            stamp_duty_paid=stamp_duty_paid,
            registration_fee=registration_fee
        )
    except ValueError as e:
        print(f"\n✗ Error: {e}")


def inherit_property_menu(ledger: PropertyBlockchain):
    """Menu for property inheritance."""
    print("\n--- INHERIT PROPERTY ---")
    print("Enter inheritance details:\n")
    
    property_key = get_input("Property Key: ")
    
    # Check if property exists
    if property_key not in ledger.property_index:
        print(f"\n✗ Error: Property '{property_key}' not found.")
        return
    
    # Show current owner
    current_state = ledger.get_property_current_state(property_key)
    print(f"\nCurrent Owner: {current_state['owner']}")
    print(f"Property Address: {current_state['address']}\n")
    
    try:
        heir_name = get_input("Heir Name: ")
        
        # Immediate validation for heir's Aadhar
        heir_aadhar = get_validated_aadhar(ledger, heir_name, "Heir's Aadhar Number (12 digits): ")
        
        # Immediate validation for heir's PAN
        heir_pan = get_validated_pan(ledger, heir_name, "Heir's PAN Number: ")
        
        # Create Owner object before the transaction
        heir = Owner(heir_name, heir_aadhar, heir_pan)
        
        relationship = get_input("Relationship to Previous Owner: ", required=False)
        legal_heir_certificate_no = get_input("Legal Heir Certificate Number: ", required=False)
        
        ledger.inherit_property(
            property_key=property_key,
            heir=heir,
            relationship=relationship,
            legal_heir_certificate_no=legal_heir_certificate_no
        )
    except ValueError as e:
        print(f"\n✗ Error: {e}")


def view_property_menu(ledger: PropertyBlockchain):
    """View property by key."""
    print("\n--- VIEW PROPERTY BY KEY ---")
    property_key = get_input("Enter Property Key: ")
    
    block_data = ledger.get_block_by_key(property_key)
    if block_data:
        print(f"\nLatest block for {property_key}:")
        print(json.dumps(block_data, indent=2))
    else:
        print(f"\n✗ Property '{property_key}' not found.")


def view_history_menu(ledger: PropertyBlockchain):
    """View property history."""
    print("\n--- VIEW PROPERTY HISTORY ---")
    property_key = get_input("Enter Property Key: ")
    
    try:
        history = ledger.get_property_history(property_key)
        print(f"\nComplete history for {property_key} ({len(history)} records):")
        for i, record in enumerate(history):
            print(f"\n  Record {i + 1}:")
            print(f"    Type: {record['data']['type']}")
            print(f"    Timestamp: {record['timestamp']}")
            if record['data']['type'] == 'registration':
                print(f"    Owner: {record['data']['owner']}")
                print(f"    Survey No: {record['data']['survey_no']}")
                print(f"    RTC No: {record['data']['rtc_no']}")
            else:
                print(f"    From: {record['data']['previous_owner']} → To: {record['data']['new_owner']}")
                print(f"    Reason: {record['data']['transfer_reason']}")
    except ValueError as e:
        print(f"\n✗ Error: {e}")


def view_current_state_menu(ledger: PropertyBlockchain):
    """View current property state."""
    print("\n--- VIEW CURRENT PROPERTY STATE ---")
    property_key = get_input("Enter Property Key: ")
    
    try:
        state = ledger.get_property_current_state(property_key)
        print(f"\nCurrent state of {property_key}:")
        print(json.dumps(state, indent=2))
    except ValueError as e:
        print(f"\n✗ Error: {e}")


def search_by_owner_menu(ledger: PropertyBlockchain):
    """Search properties by owner."""
    print("\n--- SEARCH BY OWNER ---")
    owner = get_input("Enter Owner Name: ")
    
    properties = ledger.search_by_owner(owner)
    if properties:
        print(f"\nProperties owned by {owner}: {len(properties)}")
        for prop in properties:
            print(f"\n  {prop['property_key']}:")
            print(f"    Address: {prop['address']}")
            print(f"    Survey No: {prop['survey_no']}")
            print(f"    Value: ₹{prop['value']:,.2f}")
    else:
        print(f"\n✗ No properties found for owner '{owner}'.")


def view_all_properties_menu(ledger: PropertyBlockchain):
    """View all registered properties."""
    print("\n--- ALL REGISTERED PROPERTIES ---")
    all_props = ledger.get_all_properties()
    
    if all_props:
        for prop in all_props:
            print(f"\n  {prop['property_key']}:")
            print(f"    Owner: {prop['owner']}")
            print(f"    Address: {prop['address']}")
            print(f"    Survey No: {prop['survey_no']}")
            print(f"    Value: ₹{prop['value']:,.2f}")
    else:
        print("\n  No properties registered yet.")


def main():
    """Main function to run the interactive menu."""
    print("\n" + "="*60)
    print("       INITIALIZING PROPERTY LEDGER BLOCKCHAIN")
    print("="*60)
    ledger = PropertyBlockchain()
    
    try:
        while True:
            display_menu()
            choice = input("Enter your choice (0-11): ").strip()
            
            if choice == "1":
                add_property_menu(ledger)
            elif choice == "2":
                transfer_property_menu(ledger)
            elif choice == "3":
                inherit_property_menu(ledger)
            elif choice == "4":
                view_property_menu(ledger)
            elif choice == "5":
                view_history_menu(ledger)
            elif choice == "6":
                view_current_state_menu(ledger)
            elif choice == "7":
                search_by_owner_menu(ledger)
            elif choice == "8":
                view_all_properties_menu(ledger)
            elif choice == "9":
                print("\n--- BLOCKCHAIN VALIDATION ---")
                ledger.is_chain_valid()
            elif choice == "10":
                ledger.print_chain()
            elif choice == "11":
                print("\n--- SAVE BLOCKCHAIN ---")
                ledger.save_and_exit()
            elif choice == "0":
                print("\n--- SAVING BLOCKCHAIN BEFORE EXIT ---")
                ledger.save_and_exit()
                print("\nThank you for using Property Ledger Blockchain!")
                print("Goodbye!")
                break
            else:
                print("\n✗ Invalid choice. Please enter a number between 0 and 11.")
            
            input("\nPress Enter to continue...")
    except KeyboardInterrupt:
        print("\n\n--- INTERRUPTED - SAVING BLOCKCHAIN ---")
        ledger.save_and_exit()
        print("\nBlockchain saved. Goodbye!")


if __name__ == "__main__":
    main()
