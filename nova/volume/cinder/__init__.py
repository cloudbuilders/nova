# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2011 OpenStack LLC.
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


import nova
from nova import flags
from nova.volume import cinder
from nova.openstack.common import importutils

FLAGS = flags.FLAGS


def get_default_volume_service():
    VolumeService = importutils.import_class(FLAGS.volume_service)
    return VolumeService()


def get_volume_service(context, volume_href):
    """Get the proper volume_service and id for the given volume_href.

    The volume_href param can be an href of the form
    http://mycinderserver:9292/volumes/aaa-aaa-aaaaaaa...
    , or just the uuid, specifying we use the default volume service.

    :param volume_href: volume ref/id for a volume
    :returns: a tuple of the form (volume_service, volume_id)

    """
    # check if this is not a uri
    if '/' not in str(volume_href):
        return (get_default_volume_service(), volume_href)

    else:
        (cinder_client, volume_id) = cinder.get_cinder_client(context,
                                                             volume_href)
        volume_service = nova.volume.cinder.CinderVolumeService(cinder_client)
        return (volume_service, volume_id)
