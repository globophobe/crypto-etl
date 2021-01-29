import os

import firebase_admin
from firebase_admin import firestore
from firebase_admin.credentials import Certificate

from ..constants import FIREBASE_ADMIN_CREDENTIALS, PROJECT_ID
from ..utils import is_local


class FirestoreCache:
    def __init__(self, collection):
        self.collection = collection

        if "FIREBASE_INIT" not in os.environ:
            if is_local():
                certificate = Certificate(os.environ[FIREBASE_ADMIN_CREDENTIALS])
                firebase_admin.initialize_app(certificate)
            else:
                options = {"projectId": os.environ[PROJECT_ID]}
                firebase_admin.initialize_app(options=options)
            os.environ["FIREBASE_INIT"] = "true"

        self.firestore = firestore.client()

    def is_initial(self):
        query = self.firestore.collection(self.collection).limit(1)
        results = query.stream()
        return len(list(results)) == 0

    def has_data(self, document):
        data = self.get(document)
        if data and data.get("ok", False):
            return True

    def get(self, document):
        data = self.firestore.collection(self.collection).document(document).get()
        if data:
            return data.to_dict()

    def get_one(self, where=None, order_by=None, direction=firestore.Query.ASCENDING):
        query = self.firestore.collection(self.collection)
        if where:
            query = query.where(*where)
        if order_by:
            query = query.order_by(order_by, direction=direction)
        query = query.limit(1)
        results = [r for r in query.stream()]
        if results:
            return results[0].to_dict()

    def set(self, document, data):
        self.firestore.collection(self.collection).document(document).set(data)

    def delete(self, document):
        self.firestore.collection(self.collection).document(document).delete()
