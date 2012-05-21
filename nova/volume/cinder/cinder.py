# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 OpenStack LLC.
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

"""Implementation of a volume service that uses cinder"""

from __future__ import absolute_import

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


def _parse_volume_ref(volume_href):
    """Parse a volume href into composite parts.

    :param volume_href: href of an volume
    :returns: a tuple of the form (volume_id, host, port)
    :raises ValueError

    """
    o = urlparse.urlparse(volume_href)
    port = o.port or 80
    host = o.netloc.split(':', 1)[0]
    volume_id = o.path.split('/')[-1]
    return (volume_id, host, port)


def _create_cinder_client(context, host, port):
    if FLAGS.auth_strategy == 'keystone':
        # NOTE(dprince): cinder client just needs auth_tok right? Should we
        # add username and tenant to the creds below?
        creds = {'strategy': 'keystone',
                 'username': context.user_id,
                 'tenant': context.project_id}
        cinder_client = CinderClient(host, port, auth_tok=context.auth_token,
                                     creds=creds)
    else:
        cinder_client = CinderClient(host, port)
    return cinder_client


def pick_cinder_api_server():
    """Return which cinder API server to use for the request

    This method provides a very primitive form of load-balancing suitable for
    testing and sandbox environments. In production, it would be better to use
    one IP and route that to a real load-balancer.

        Returns (host, port)
    """
    host_port = random.choice(FLAGS.cinder_api_servers)
    host, port_str = host_port.split(':')
    port = int(port_str)
    return host, port


def get_cinder_client(context, volume_href):
    """Get the correct cinder client and id for the given volume_href.

    The volume_href param can be an href of the form
    http://mycinderserver:9292/volumes/42, or just an int such as 42. If the
    volume_href is an int, then flags are used to create the default
    cinder client.

    :param volume_href: volume ref/id for an volume
    :returns: a tuple of the form (cinder_client, volume_id)

    """
    (cinder_host, cinder_port) = pick_cinder_api_server()

    # check if this is an id
    if '/' not in str(volume_href):
        cinder_client = _create_cinder_client(context,
                                              cinder_host,
                                              cinder_port)
        return (cinder_client, volume_href)

    else:
        try:
            (volume_id, cinder_host, cinder_port) =\
                _parse_volume_ref(volume_href)
            cinder_client = _create_cinder_client(context,
                                                  cinder_host,
                                                  cinder_port)
        except ValueError:
            raise exception.InvalidvolumeRef(volume_href=volume_href)

        return (cinder_client, volume_id)


class cindervolumeService(object):
    """Provides storage and retrieval of disk volume objects within cinder."""

    def __init__(self, client=None):
        self._client = client

    def _get_client(self, context):
        # NOTE(sirp): we want to load balance each request across cinder
        # servers. Since cindervolumeService is a long-lived object, `client`
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

        raise exception.cinderConnectionFailed(
                reason=_('Maximum attempts reached'))

    def index(self, context, **kwargs):
        """Calls out to cinder for a list of volumes available."""
        params = self._extract_query_params(kwargs)
        volume_metas = self._get_volumes(context, **params)

        volumes = []
        for volume_meta in volume_metas:
            if self._is_volume_available(context, volume_meta):
                meta_subset = utils.subset_dict(volume_meta, ('id', 'name'))
                volumes.append(meta_subset)
        return volumes

    def detail(self, context, **kwargs):
        """Calls out to cinder for a list of detailed volume information."""
        params = self._extract_query_params(kwargs)
        volume_metas = self._get_volumes(context, **params)

        volumes = []
        for volume_meta in volume_metas:
            if self._is_volume_available(context, volume_meta):
                base_volume_meta = self._translate_from_cinder(volume_meta)
                volumes.append(base_volume_meta)
        return volumes

    def _extract_query_params(self, params):
        _params = {}
        accepted_params = ('filters', 'marker', 'limit',
                           'sort_key', 'sort_dir')
        for param in accepted_params:
            if param in params:
                _params[param] = params.get(param)

        return _params

    def _get_volumes(self, context, **kwargs):
        """Get volume entitites from volumes service"""

        client = self._get_client(context)
        return self._fetch_volumes(client.get_volumes_detailed, **kwargs)

    def _fetch_volumes(self, fetch_func, **kwargs):
        """Paginate through results from cinder server"""
        try:
            volumes = fetch_func(**kwargs)
        except Exception:
            _reraise_translated_exception()

        if not volumes:
            # break out of recursive loop to end pagination
            return

        for volume in volumes:
            yield volume

        try:
            # attempt to advance the marker in order to fetch next page
            kwargs['marker'] = volumes[-1]['id']
        except KeyError:
            raise exception.volumePaginationFailed()

        try:
            kwargs['limit'] = kwargs['limit'] - len(volumes)
            # break if we have reached a provided limit
            if kwargs['limit'] <= 0:
                return
        except KeyError:
            # ignore missing limit, just proceed without it
            pass

        for volume in self._fetch_volumes(fetch_func, **kwargs):
            yield volume

    def show(self, context, volume_id):
        """Returns a dict with volume data for the given opaque volume id."""
        try:
            volume_meta = self._call_retry(context, 'get_volume_meta',
                                          volume_id)
        except Exception:
            _reraise_translated_volume_exception(volume_id)

        if not self._is_volume_available(context, volume_meta):
            raise exception.volumeNotFound(volume_id=volume_id)

        base_volume_meta = self._translate_from_cinder(volume_meta)
        return base_volume_meta

    def show_by_name(self, context, name):
        """Returns a dict containing volume data for the given name."""
        volume_metas = self.detail(context, filters={'name': name})
        try:
            return volume_metas[0]
        except (IndexError, TypeError):
            raise exception.volumeNotFound(volume_id=name)

    def get(self, context, volume_id, data):
        """Calls out to cinder for metadata and data and writes data."""
        try:
            volume_meta, volume_chunks = self._call_retry(context,
                                                          'get_volume',
                                                          volume_id)
        except Exception:
            _reraise_translated_volume_exception(volume_id)

        for chunk in volume_chunks:
            data.write(chunk)

        base_volume_meta = self._translate_from_cinder(volume_meta)
        return base_volume_meta

    def create(self, context, volume_meta, data=None):
        """Store the volume data and return the new volume id.

        :raises: AlreadyExists if the volume already exist.

        """
        # Translate Base -> Service
        LOG.debug(_('Creating volume in cinder. Metadata passed in %s'),
                  volume_meta)
        sent_service_volume_meta = self._translate_to_cinder(volume_meta)
        LOG.debug(_('Metadata after formatting for cinder %s'),
                  sent_service_volume_meta)

        recv_service_volume_meta = self._get_client(context).add_volume(
            sent_service_volume_meta, data)

        # Translate Service -> Base
        base_volume_meta =\
                self._translate_from_cinder(recv_service_volume_meta)
        LOG.debug(_('Metadata returned from cinder formatted for Base %s'),
                  base_volume_meta)
        return base_volume_meta

    def update(self, context, volume_id, volume_meta, data=None):
        """Replace the contents of the given volume with the new data.

        :raises: volumeNotFound if the volume does not exist.

        """
        # NOTE(vish): show is to check if volume is available
        self.show(context, volume_id)
        volume_meta = self._translate_to_cinder(volume_meta)
        client = self._get_client(context)
        try:
            volume_meta = client.update_volume(volume_id, volume_meta, data)
        except Exception:
            _reraise_translated_volume_exception(volume_id)

        base_volume_meta = self._translate_from_cinder(volume_meta)
        return base_volume_meta

    def delete(self, context, volume_id):
        """Delete the given volume.

        :raises: volumeNotFound if the volume does not exist.
        :raises: NotAuthorized if the user is not an owner.

        """
        # NOTE(vish): show is to check if volume is available
        volume_meta = self.show(context, volume_id)

        if FLAGS.auth_strategy == 'deprecated':
            # NOTE(parthi): only allow volume deletions if the user
            # is a member of the project owning the volume, in case of
            # setup without keystone
            # TODO(parthi): Currently this access control breaks if
            # 1. volume is not owned by a project
            # 2. Deleting user is not bound a project
            properties = volume_meta['properties']
            if (context.project_id and ('project_id' in properties)
                and (context.project_id != properties['project_id'])):
                raise exception.NotAuthorized(_("Not the volume owner"))

            if (context.project_id and ('owner_id' in properties)
                and (context.project_id != properties['owner_id'])):
                raise exception.NotAuthorized(_("Not the volume owner"))

        try:
            result = self._get_client(context).delete_volume(volume_id)
        except cinder_exception.NotFound:
            raise exception.volumeNotFound(volume_id=volume_id)
        return result

    def delete_all(self):
        """Clears out all volumes."""
        pass

    @classmethod
    def _translate_to_cinder(cls, volume_meta):
        volume_meta = _convert_to_string(volume_meta)
        return volume_meta

    @classmethod
    def _translate_from_cinder(cls, volume_meta):
        volume_meta = _limit_attributes(volume_meta)
        volume_meta = _convert_timestamps_to_datetimes(volume_meta)
        volume_meta = _convert_from_string(volume_meta)
        return volume_meta

    @staticmethod
    def _is_volume_available(context, volume_meta):
        """Check volume availability.

        Under cinder, volumes are always available if the context has
        an auth_token.

        """
        if hasattr(context, 'auth_token') and context.auth_token:
            return True

        if context.is_admin:
            return True

        properties = volume_meta['properties']

        if context.project_id and ('owner_id' in properties):
            return str(properties['owner_id']) == str(context.project_id)

        if context.project_id and ('project_id' in properties):
            return str(properties['project_id']) == str(context.project_id)

        try:
            user_id = properties['user_id']
        except KeyError:
            return False

        return str(user_id) == str(context.user_id)


# utility functions
def _convert_timestamps_to_datetimes(volume_meta):
    """Returns volume with timestamp fields converted to datetime objects."""
    for attr in ['created_at', 'updated_at', 'deleted_at']:
        if volume_meta.get(attr):
            volume_meta[attr] = _parse_cinder_iso8601_timestamp(
                volume_meta[attr])
    return volume_meta


def _parse_cinder_iso8601_timestamp(timestamp):
    """Parse a subset of iso8601 timestamps into datetime objects."""
    iso_formats = ['%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S']

    for iso_format in iso_formats:
        try:
            return datetime.datetime.strptime(timestamp, iso_format)
        except ValueError:
            pass

    raise ValueError(_('%(timestamp)s does not follow any of the '
                       'signatures: %(iso_formats)s') % locals())


# TODO(yamahata): use block-device-mapping extension to cinder
def _json_loads(properties, attr):
    prop = properties[attr]
    if isinstance(prop, basestring):
        properties[attr] = json.loads(prop)


def _json_dumps(properties, attr):
    prop = properties[attr]
    if not isinstance(prop, basestring):
        properties[attr] = json.dumps(prop)


_CONVERT_PROPS = ('block_device_mapping', 'mappings')


def _convert(method, metadata):
    metadata = copy.deepcopy(metadata)  # don't touch original metadata
    properties = metadata.get('properties')
    if properties:
        for attr in _CONVERT_PROPS:
            if attr in properties:
                method(properties, attr)

    return metadata


def _convert_from_string(metadata):
    return _convert(_json_loads, metadata)


def _convert_to_string(metadata):
    return _convert(_json_dumps, metadata)


def _limit_attributes(volume_meta):
    volume_ATTRIBUTES = ['id', 'size', 'display_name',
                         'created_at', 'updated_at',
                         'deleted_at', 'deleted',
                         'status', 'atached']
    output = {}
    for attr in volume_ATTRIBUTES:
        output[attr] = volume_meta.get(attr)

    output['properties'] = volume_meta.get('properties', {})

    return output


def _reraise_translated_volume_exception(volume_id):
    """Transform the exception for the volume but keep its traceback intact."""
    exc_type, exc_value, exc_trace = sys.exc_info()
    new_exc = _translate_volume_exception(volume_id, exc_type, exc_value)
    raise new_exc, None, exc_trace


def _reraise_translated_exception():
    """Transform the exception but keep its traceback intact."""
    exc_type, exc_value, exc_trace = sys.exc_info()
    new_exc = _translate_plain_exception(exc_type, exc_value)
    raise new_exc, None, exc_trace


def _translate_volume_exception(volume_id, exc_type, exc_value):
    if exc_type in (cinder_exception.Forbidden,
                    cinder_exception.NotAuthenticated,
                    cinder_exception.MissingCredentialError):
        return exception.volumeNotAuthorized(volume_id=volume_id)
    if exc_type is cinder_exception.NotFound:
        return exception.volumeNotFound(volume_id=volume_id)
    if exc_type is cinder_exception.Invalid:
        return exception.Invalid(exc_value)
    return exc_value


def _translate_plain_exception(exc_type, exc_value):
    if exc_type in (cinder_exception.Forbidden,
                    cinder_exception.NotAuthenticated,
                    cinder_exception.MissingCredentialError):
        return exception.NotAuthorized(exc_value)
    if exc_type is cinder_exception.NotFound:
        return exception.NotFound(exc_value)
    if exc_type is cinder_exception.Invalid:
        return exception.Invalid(exc_value)
    return exc_value
