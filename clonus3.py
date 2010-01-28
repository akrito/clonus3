#!/usr/bin/env python

from boto.s3 import Bucket, Connection, Key
from optparse import OptionParser
from time import time
import anydbm
import os
import re
import shutil
import simplejson
import sys
import yaml


class Spinner:

    def __init__(self):
        self.status = 0
        self.locations = ['|', '/', '-', '\\']

    def spin(self):
        sys.stderr.write("%s\r" % self.locations[self.status])
        sys.stderr.flush()
        self.status = (self.status + 1) % 4


class BackupActor:

    def __init__(self, options, settings_file):
        self.options = options
        f = open(settings_file)
        self.settings = yaml.load(f.read())
        f.close()
        self.bucket_name = self.settings['bucket']
        self.spinner = Spinner()

        self.client = Connection(
            aws_access_key_id = self.settings['access_key_id'],
            aws_secret_access_key = self.settings['secret_access_key'],
            is_secure = False
        )

        db_location = self.settings.get('cache', None)
        if db_location:
            self.db = anydbm.open(db_location, 'c')
        else:
            self.db = None

    def say(self, str):
        if not self.options.quiet:
            sys.stdout.write(str)
            sys.stdout.flush()

    def s3path(self, root, path):
        if self.settings.get('relative_paths', False):
            return path[len(root) + 1:]
        return path[1:]

    def filepath(self, key):
        if self.settings.get('relative_paths', False):
            return self.settings['roots'][0] + '/' + key
        return '/' + key

    def head(self, root, path):
        """
        Perform a head request, optionally caching
        """
        # First, check the cache
        if not self.db is None:
            headers_pickled = self.db.get(self.s3path(root, path), None)
            if headers_pickled:
                return simplejson.loads(headers_pickled)
        # If no cache hit, HEAD S3
        k = self.bucket.get_key(self.s3path(root, path))
        if k:
            headers = {'mtime': k.get_metadata('mtime'), 'size': str(k.size), 'etag': k.etag }
        else:
            headers = None

        # Save headers to the cache
        if (not self.db is None) and headers:
            self.db[self.s3path(root, path)] = simplejson.dumps(headers)

        return headers

    def backup(self):
        # Create and/or update the bucket's acl
        if self.settings.has_key('bucket_acl'):
            self.client.create_bucket(self.bucket_name, policy = self.settings['bucket_acl'])
        else:
            self.client.create_bucket(self.bucket_name)
        self.bucket = Bucket(self.client, self.bucket_name)

        # Only list the contents of the bucket if we have to
        if self.options.delete or ((not self.db is None) and self.options.rebuildcache):
            self.scan_s3()

        for root in sorted(self.settings['roots']):
            self.walk(root)
        self.say("\n")

    def walk(self, root):
        for dirpath, dirnames, filenames in os.walk(root):
            self.say("  Scanning %s\n" % dirpath)

            for entry in filenames:
                path = os.path.join(dirpath, entry)
                if any(re.search(x, path) for x in self.settings['ignore']):
                    continue
                if os.path.islink(path):
                    continue
                try:
                    mtime = str(int(os.path.getmtime(path)))
                    size = str(os.path.getsize(path))
                    headers = self.head(root, path)
                    if headers:
                        if mtime != headers['mtime'] or size != headers['size']:
                            self.timed_store(root, path, "+ Updating %s - %s" % (path, size))
                        else:
                            if not self.options.quiet:
                                self.spinner.spin()
                    else:
                        self.timed_store(root, path, "! Uploading %s - %s" % (path, size))

                except Exception, e:
                    sys.stderr.write("Could not back up %s: %s\n" % (path, e))

    def timed_store(self, root, path, text):
        size = os.path.getsize(path)
        mtime = int(os.path.getmtime(path))

        self.say(text)

        if not self.options.dryrun:
            # Clear the cache
            if (not self.db is None) and self.db.has_key(self.s3path(root, path)):
                del self.db[self.s3path(root, path)]

            t1 = time()
            k = Key(bucket = self.bucket, name = self.s3path(root, path))
            k.set_metadata('mtime', str(mtime))
            acl = self.settings.get('object_acl', None)
            if acl:
                k.set_acl(acl)
            k.set_contents_from_filename(path)
            if acl:
                k.set_acl(acl)
            t = time() - t1
            self.say(" in %.2fs [%.2fKB/s]\n" % (t, (size / 1000.0) / t))
        else:
            self.say(" (dry run)\n")

    def scan_s3(self):
        self.say("  Listing bucket\n")

        if (not self.db is None) and self.options.rebuildcache:
            newdb_name = self.settings['cache'] + '.tmp'
            newdb = anydbm.open(newdb_name, 'c')

        for k in self.bucket.list():
            self.spinner.spin()
            key = k.name

            # Does the S3 object still exist on the filesystem?

            if self.options.delete:
                # TODO: make sure the file is in a valid root and
                # doesn't match an ignored regex
                if not os.path.exists(self.filepath(key)):
                    self.say("- Unstoring %s" % key)
                    if self.options.dryrun:
                        self.say(" (dry run)\n")
                    else:
                        self.say("\n")
                        # Clear the cache
                        if (not self.db is None) and self.db.has_key(key):
                            del self.db[key]
                        self.bucket.delete_key(key)

            # Remove out-of-date cached HEAD responses

            if (not self.db is None) and self.options.rebuildcache:
                cached_headers_pickled = self.db.get(key, None)
                if cached_headers_pickled:
                    cached_headers = simplejson.loads(cached_headers_pickled)
                    if cached_headers['etag'] == k.etag:
                        newdb[key] = cached_headers_pickled
                    else:
                        sys.stderr.write("S3 does not match cache.  Who messed with my bucket?\n")

        # set @db to the new cache

        if (not self.db is None) and self.options.rebuildcache:
            newdb.close()
            self.db.close()
            shutil.move(newdb_name, self.settings['cache'])
            self.db = anydbm.open(self.settings['cache'], 'w')

if __name__ == '__main__':
    parser = OptionParser()
    options_a = [
        ["-q", "--quiet", dict(dest="quiet", action="store_true", default=False, help="be quiet")],
        ["-d", "--delete", dict(dest="delete", action="store_true", default=False, help="First, remove files which won't be backed up from S3")],
        ["-n", "--dry-run", dict(dest="dryrun", action="store_true", default=False, help="Connect to S3 and create the bucket, but don't upload or remove files")],
        ["-r", "--no-rebuild-cache", dict(dest="rebuildcache", action="store_false", default=True, help="Don't rebuild the cache.  Rebuilding tosses out stale HEAD responses.")],
    ]
    for s, l, k in options_a:
        parser.add_option(s, l, **k)
    (options, args) = parser.parse_args()
    b = BackupActor(options, args[0])
    b.backup()
