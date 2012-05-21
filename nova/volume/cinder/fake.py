# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 Justin Santa Barbara
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

"""Implementation of an fake volume service"""

import copy
import datetime

from nova import exception
from nova import flags
from nova import log as logging
from nova import utils


LOG = logging.getLogger(__name__)


FLAGS = flags.FLAGS


class _FakeVolumeService(object):
    """Mock (fake) volume service for unit testing."""

    def __init__(self):
        self.volumes = {}
        timestamp = datetime.datetime(2012, 01, 01, 01, 02, 03)

        fake_uuid_1 = '11111111-aaaa-bbbb-cccc-1111aaaa3333'
        fake_uuid_2 = '22222222-aaaa-bbbb-cccc-2222aaaa3333'
        fake_uuid_3 = '22222222-aaaa-bbbb-cccc-3333aaaa3333'

        volume_1 = {'id': fake_uuid_1,
                 'created_at': timestamp,
                 'updated_at': timestamp,
                 'deleted_at': None,
                 'deleted': False,
                 'user_id': 'fakeuser',
                 'project_id': 'fakeproject',
                 'snapshot_id': None,
                 'host': 'fakehost',
                 'size': 1,
                 'availability_zone': 'fakeaz',
                 'instance_uuid': None,
                 'mountpoint': None,
                 'attach_time': None,
                 'status': 'active',
                 'attach_status': 'detached',
                 'scheduled_at': '',
                 'launched_at': '',
                 'terminated_at': '',
                 'display_name': 'fakevol-1',
                 'display_description': 'fakedescription',
                 'provider_location': '',
                 'provider_auth': '',
                 'volume_type_id': 'faketypeid'}

        volume_2 = {'id': fake_uuid_2,
                 'created_at': timestamp,
                 'updated_at': timestamp,
                 'deleted_at': None,
                 'deleted': False,
                 'user_id': 'fakeuser',
                 'project_id': 'fakeproject',
                 'snapshot_id': None,
                 'host': 'fakehost',
                 'size': 1,
                 'availability_zone': 'fakeaz',
                 'instance_uuid': None,
                 'mountpoint': None,
                 'attach_time': None,
                 'status': 'active',
                 'attach_status': 'detached',
                 'scheduled_at': '',
                 'launched_at': '',
                 'terminated_at': '',
                 'display_name': 'fakevol-1',
                 'display_description': 'fakedescription',
                 'provider_location': '',
                 'provider_auth': '',
                 'volume_type_id': 'faketypeid'}

        volume_3 = {'id': fake_uuid_3,
                 'created_at': timestamp,
                 'updated_at': timestamp,
                 'deleted_at': None,
                 'deleted': False,
                 'user_id': 'fakeuser',
                 'project_id': 'fakeproject',
                 'snapshot_id': None,
                 'host': 'fakehost',
                 'size': 1,
                 'availability_zone': 'fakeaz',
                 'instance_uuid': None,
                 'mountpoint': None,
                 'attach_time': None,
                 'status': 'active',
                 'attach_status': 'detached',
                 'scheduled_at': '',
                 'launched_at': '',
                 'terminated_at': '',
                 'display_name': 'fakevol-1',
                 'display_description': 'fakedescription',
                 'provider_location': '',
                 'provider_auth': '',
                 'volume_type_id': 'faketypeid'}

        self.create(None, volume_1)
        self.create(None, volume_2)
        self.create(None, volume_3)
        self._volumedata = {}
        super(_FakeVolumeService, self).__init__()

    def index(self, context, **kwargs):
        """Returns list of volumes."""
        retval = []
        for v in self.volumes.values():
            retval += [dict([(k, v) for k, v in v.iteritems()
                                                  if k in ['id', 'name']])]
        return retval

    def detail(self, context, **kwargs):
        """Return list of detailed volume information."""
        return copy.deepcopy(self.volumes.values())

    def get(self, context, volume_id, data):
        metadata = self.show(context, volume_id)
        data.write(self._volumedata.get(volume_id, ''))
        return metadata

    def show(self, context, volume_id):
        """Get data about specified volume.

        Returns a dict containing volume data for the given opaque volume id.

        """
        volume = self.volumes.get(str(volume_id))
        if volume:
            return copy.deepcopy(volume)
        LOG.warn('Unable to find volume id %s.  Have volumes: %s',
                 volume_id, self.volumes)
        raise exception.VolumeNotFound(volume_id=volume_id)

    def show_by_name(self, context, name):
        """Returns a dict containing volume data for the given name."""
        volumes = copy.deepcopy(self.volumes.values())
        for v in volumes:
            if name == v.get('name'):
                return v
        raise exception.VolumeNotFound(volume_id=name)

    def create(self, context, metadata, data=None):
        """Store the volume data and return the new volume id.

        """
        volume_id = str(metadata.get('id', utils.gen_uuid()))
        metadata['id'] = volume_id

        self.volumes[volume_id] = copy.deepcopy(metadata)
        if data:
            self._volumedata[volume_id] = data.read()
        return self.volumes[volume_id]

    def update(self, context, volume_id, metadata, data=None):
        """Replace the contents of the given volume with the new data.

        :raises: VolumeNotFound if the volume does not exist.

        """
        if not self.volumes.get(volume_id):
            raise exception.VolumeNotFound(volume_id=volume_id)
        self.volumes[volume_id] = copy.deepcopy(metadata)

    def delete(self, context, volume_id):
        """Delete the given volume.

        :raises: VolumeNotFound if the volume does not exist.

        """
        removed = self.volumes.pop(volume_id, None)
        if not removed:
            raise exception.VolumeNotFound(volume_id=volume_id)

    def delete_all(self):
        """Clears out all volumes."""
        self.volumes.clear()

_fakeVolumeService = _FakeVolumeService()


def FakeVolumeService():
    return _fakeVolumeService


def FakeVolumeService_reset():
    global _fakeVolumeService
    _fakeVolumeService = _FakeVolumeService()
