# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2012 SolidFire Inc.
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

import copy
import datetime
import json
import random
import sys
import time
import urlparse

from cinder.common import exception as cinder_exception

from nova import exception
from nova import flags
from nova import log as logging
from nova.openstack.common import importutils
from nova import utils

LOG = logging.getLogger(__name__)

FLAGS = flags.FLAGS

CinderClient = importutils.import_class('cinder.client.Client')

def _create_cinder_client(volume_href):
    pass

def _parse_volume_ref(volume_href):
    pass



def pick_cinder_api_server():
    """Return which Cinder API server to use for the request

    This is very simplistic and should be replaced by a real load
    balancer in production environments.

        Returns: host, port
    """
    host_port = random.choice(FLAGS.cinder_api_servers)
    (host, port_str) = host_port.split(':')
    port = int(port_str)
    return (host, port)


def get_cinder_client(context, volume_href):
    """Get the correct cinder client and id for the given volume_href.

    The volume_href param can be an href of the form
    http://mycinderserver:9292/images/42, or just an int such as 42. If the
    volume_href is an int, then flags are used to create the default
    cinder client.

    :param volume_href: image ref/id for an image
    :returns: a tuple of the form (cinder_client, image_id)

    """
    cinder_host, cinder_port = pick_cinder_api_server()

    # check if this is an id
    if '/' not in str(volume_href):
        cinder_client = _create_cinder_client(context,
                                              cinder_host,
                                              cinder_port)
        return (cinder_client, volume_href)

    else:
        try:
            (image_id, cinder_host, cinder_port) =\
                    _parse_volume_ref(volume_href)
            cinder_client = _create_cinder_client(context,
                                                  cinder_host,
                                                  cinder_port)
        except ValueError:
            raise exception.InvalidVolumeRef(volume_href=volume_href)

        return (cinder_client, image_id)


class CinderVolumeService(object):
    """Provides storage and retrieval of disk image objects within Cinder."""

    def __init__(self, client=None):
        self._client = client

    def _get_client(self, context):
        # NOTE(sirp): we want to load balance each request across cinder
        # servers. Since CinderVolumeService is a long-lived object, `client`
        # is made to choose a new server each time via this property.
        if self._client is not None:
            return self._client
        cinder_host, cinder_port = pick_cinder_api_server()
        return _create_cinder_client(context, cinder_host, cinder_port)

    def _call_retry(self, context, name, *args, **kwargs):
        """Retry call to cinder server if there is a connection error.
        Suitable only for idempotent calls."""
        for i in xrange(FLAGS.cinder_num_retries + 1):
            client = self._get_client(context)
            try:
                return getattr(client, name)(*args, **kwargs)
            except cinder_exception.ClientConnectionError as e:
                LOG.exception(_('Connection error contacting cinder'
                                ' server, retrying'))

                time.sleep(1)

        raise exception.CinderConnectionFailed(
                reason=_('Maximum attempts reached'))

    def create(self, context, size, name, description,
               snapshot=None, volume_type=None, metadata=None,
               availability_zone=None):
        pass

    def wait_create(self, context, volume):
        pass

    def delete(self, context, volume):
        pass

    def get(self, context, volume_id):
        pass

    def get_all(self, context, search_opts={}):
        pass

    def get_snapshot(self, context, snapshot_id):
        pass

    def get_all_snapshots(self, context):
        pass

    def check_attach(self, context, volume):
        pass

    def check_detach(self, context, volume):
        pass

    def remove_from_compute(self, context, volume,
                            instance_id, host):
        """Remove volume from specified compute host."""
        #NOTE(jdg): have to look at where this goes
        pass

    def reserve_volume(self, context, volume):
        pass

    def unreserve_volume(self, context, volume):
        pass

    def attach(self, context, volume, instance_uuid, mountpoint):
        pass

    def detach(self, context, volume):
        pass

    def initialize_connection(self, context, volume, connector):
        pass

    def terminate_connection(self, context, volume, connector):
        pass

    def _create_snapshot(self, context, volume,
                         name, description,
                         force=False):
        pass

    def create_snapshot(self, context, volume, name, description):
        pass

    def delete_snapshot(self, context, snapshot):
        pass

    def get_volume_metadata(self, context, volume):
        pass

    def delete_volume_metadata(self, context, volume, key):
        pass

    def update_volume_metadata(self, context, volume, metadata, delete=False):
        pass

    def get_volume_metadata_value(self, volume, key):
        pass

