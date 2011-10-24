# Copyright 2011 Eldar Nugaev
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import json
import webob

from nova import compute
from nova import rpc
from nova import test
from nova.tests.api.openstack import fakes

from nova.api.openstack.contrib.console_output import Console_output


def fake_text_console(self, method, context, instance_id):
    return 'foo'


def fake_text_console_tail(self, method, context, instance_id):
    return 'foo\nbar\nbaz\nwibble\nwobble\nflub'


class ConsoleOutputExtensionTest(test.TestCase):

    def setUp(self):
        super(ConsoleOutputExtensionTest, self).setUp()

    def test_get_text_console_instance_action(self):
        self.stubs.Set(compute.API, '_call_compute_message',
                       fake_text_console)

        body = dict(getConsoleOutput=dict())
        req = webob.Request.blank('/v1.1/123/servers/1/action')
        req.method = "POST"
        req.body = json.dumps(body)
        req.headers["content-type"] = "application/json"

        res = req.get_response(fakes.wsgi_app())
        self.assertEqual(res.status_int, 200)

    def test_get_console_output_with_tail(self):
        self.stubs.Set(compute.API, '_call_compute_message',
                       fake_text_console)

        body = dict(getConsoleOutput=dict(length=3))
        req = webob.Request.blank('/v1.1/123/servers/1/action')
        req.method = "POST"
        req.body = json.dumps(body)
        req.headers["content-type"] = "application/json"

        res = req.get_response(fakes.wsgi_app())
        self.assertEqual(res.status_int, 200)

