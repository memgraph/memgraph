# -*- coding: utf-8 -*-

import unittest
import requests


class VertexCrudTest(unittest.TestCase):

    def test_crud(self):
        '''
        create -> read -> update -> read -> delete -> read
        '''
        endpoint = 'http://localhost:7474/db/data/node'
        payload = { "foo1": "bar1", "foo2": "bar2" }
        r = requests.post(endpoint, json = payload)
        self.assertEqual(r.status_code, 201)
        response = r.json()
        self.assertIn("metadata", response)
        metadata = response["metadata"]
        self.assertIn("id", metadata)
        identifier = metadata["id"]
        self.assertIn("data", response)
        data = response["data"]
        self.assertDictEqual(payload, data)
        
        # TODO: read -> update -> read -> delete -> read
