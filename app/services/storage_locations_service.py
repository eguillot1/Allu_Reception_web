"""Storage locations service (extracted constant)."""

STORAGE_DROPDOWN_IDS = {
    "-80°C": "d2a04fdb-cf6e-4cda-8d21-775c0c42e193",
    "-20°C": "53fdbee7-ef48-4aaa-91b2-a910efc89419",
    "4°C": "b354d914-5cda-4004-a088-13a553992c93",
    "Room Temperature": "b33860e2-d7c4-4169-af55-bb34b018ca8e",
}

STORAGE_LOCATIONS = {
    "-80°C": ["Main -80°C", "-80°C Garage"],
    "-20°C": ["-20°C Left", "-20°C Right"],
    "4°C": ["Glass Door 4°C", "Lab Mini 4°C", "Machine Mini 4°C", "Storage Room 4°C", "BSL2 Mini 4°C"],
    "Room Temperature": [
        "Chemical Shelf", "Solvent Cabinet", "Acid Cabinet", "Base Cabinet",
        "Storage Room", "Wet Lab", "Other"
    ],
}

__all__ = ["STORAGE_LOCATIONS", "STORAGE_DROPDOWN_IDS"]
