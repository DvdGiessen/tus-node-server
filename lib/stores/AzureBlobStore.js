'use strict';

const assert = require('assert');
const DataStore = require('./DataStore');
const File = require('../models/File');
const {
    Aborter,
    BlockBlobURL,
    ContainerURL,
    ServiceURL,
    StorageURL,
    SharedKeyCredential,
} = require('@azure/storage-blob');
const ERRORS = require('../constants').ERRORS;
const EVENTS = require('../constants').EVENTS;
const TUS_RESUMABLE = require('../constants').TUS_RESUMABLE;
const debug = require('debug');
const log = debug('tus-node-server:stores:azureblobstore');

/**
 * @fileOverview
 * Store using the Azure Storage SDK
 *
 * @author Daniël van de Giessen <daniel@dvdgiessen.nl>
 */

class AzureBlobStore extends DataStore {
    constructor(options) {
        super(options);
        this.extensions = ['creation', 'creation-defer-length'];

        assert.ok(options.account, '[AzureBlobStore] `account` must be set');
        assert.ok(options.accountKey, '[AzureBlobStore] `accountKey` must be set');
        assert.ok(options.containerName, '[AzureBlobStore] `containerName` must be set');

        const credentials = new SharedKeyCredential(
            options.account,
            options.accountKey
        );
        const pipeline = StorageURL.newPipeline(credentials);
        const serviceURL = new ServiceURL(
            `https://${options.account}.blob.core.windows.net`,
            pipeline
        );
        this.containerURL = ContainerURL.fromServiceURL(
            serviceURL,
            options.containerName
        );

        // cache object to save upload data
        // avoiding multiple http calls to azure
        this.cache = {};
    }

    _saveMetadata(blobURL, file) {
        this.cache[blobURL.url] = { file };
        return blobURL.setMetadata(Aborter.none, {
            tus_version: TUS_RESUMABLE,
            file: JSON.stringify(file),
        }, {});
    }

    _getMetadata(blobURL) {
        if (this.cache[blobURL.url]) {
            return Promise.resolve(this.cache[blobURL.url]);
        }
        return blobURL.getProperties(Aborter.none)
            .then((data) => {
                return (this.cache[blobURL.url] = {
                    file: JSON.parse(data.metadata.file),
                });
            });
    }

    _invalidateMetadataCache(blobURL) {
        delete this.cache[blobURL.url];
    }

    _retrieveBlocks(blockBlobURL) {
        return blockBlobURL.getBlockList(Aborter.none, 'uncommitted')
            .then((data) => {
                return data.uncommittedBlocks;
            });
    }

    /**
     * Create an empty file.
     *
     * @param  {object}  req  http.incomingMessage
     * @return {Promise}
     */
    create(req) {
        return new Promise((resolve, reject) => {
            const upload_length = req.headers['upload-length'];
            const upload_defer_length = req.headers['upload-defer-length'];
            const upload_metadata = req.headers['upload-metadata'];

            if (
                upload_length === undefined &&
                upload_defer_length === undefined
            ) {
                return reject(ERRORS.INVALID_LENGTH);
            }

            let file_id;
            try {
                file_id = this.generateFileName(req);
            }
            catch (generateError) {
                log(
                    '[AzureBlobStore] create: check your namingFunction. Error',
                    generateError
                );
                return reject(ERRORS.FILE_WRITE_ERROR);
            }

            const blockBlobURL = BlockBlobURL.fromContainerURL(this.containerURL, file_id);
            const file = new File(
                file_id,
                upload_length,
                upload_defer_length,
                upload_metadata
            );

            return blockBlobURL.upload(Aborter.none, '', 0)
                .then(() => {
                    return this._saveMetadata(blockBlobURL, file);
                })
                .then(() => {
                    this.emit(EVENTS.EVENT_FILE_CREATED, { file });
                    return resolve(file);
                })
                .catch((err) => {
                    this._invalidateMetadataCache(blockBlobURL);
                    return reject(err);
                });
        });
    }

    /**
     * Write to the file, starting at the provided offset
     *
     * @param  {object}  req     http.incomingMessage
     * @param  {string}  file_id Name of file
     * @param  {integer} offset  starting offset
     * @return {Promise}
     */
    write(req, file_id, offset) {
        const blockBlobURL = BlockBlobURL.fromContainerURL(this.containerURL, file_id);
        return this._getMetadata(blockBlobURL).then((metadata) => {
            const blockId = Buffer.from(offset.toString().padStart(64, '0')).toString('base64');
            const length = parseInt(req.headers['content-length'], 10);
            return blockBlobURL.stageBlock(Aborter.none, blockId, () => req, length)
                .then(() => {
                    const newOffset = offset + length;
                    if (metadata.file.upload_length > newOffset) {
                        return newOffset;
                    }
                    return this._retrieveBlocks(blockBlobURL).then((blocks) => {
                        const blockList = blocks
                            .sort((a, b) =>
                                parseInt(Buffer.from(a.name, 'base64').toString(), 10) - parseInt(Buffer.from(b.name, 'base64').toString(), 10)
                            )
                            .map((x) => x.name);
                        return blockBlobURL.commitBlockList(Aborter.none, blockList, {
                            metadata: {
                                file: JSON.stringify(metadata.file),
                            },
                        })
                            .then(() => {
                                this._invalidateMetadataCache(blockBlobURL);
                                this.emit(EVENTS.EVENT_UPLOAD_COMPLETE, {
                                    file: Object.assign({}, metadata.file, { location: blockBlobURL.url }),
                                });
                                return newOffset;
                            });
                    });
                });
        });
    }

    /**
     * Return file stats, if they exits
     *
     * @param  {string} file_id name of the file
     * @return {object}         fs stats
     */
    getOffset(file_id) {
        return new Promise((resolve, reject) => {
            const blockBlobURL = BlockBlobURL.fromContainerURL(this.containerURL, file_id);
            Promise
                .all([
                    this._getMetadata(blockBlobURL),
                    this._retrieveBlocks(blockBlobURL),
                ])
                .then(([metadata, blocks]) => {
                    return resolve(Object.assign({}, metadata.file, {
                        size: blocks.reduce((sum, block) => {
                            return sum + block.size;
                        }, 0),
                    }));
                })
                .catch((err) => {
                    this._invalidateMetadataCache(blockBlobURL);
                    if (err.statusCode === 404) {
                        return reject(ERRORS.FILE_NOT_FOUND);
                    }
                    throw err;
                });
        });
    }
}

module.exports = AzureBlobStore;
