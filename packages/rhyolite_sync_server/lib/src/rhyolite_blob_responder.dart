import 'dart:async';

import 'package:async/async.dart';
import 'package:rhyolite_sync/rhyolite_sync.dart';
import 'package:rpc_blob/rpc_blob.dart';
import 'package:rpc_dart/rpc_dart.dart';

class RhyoliteBlobResponder extends BlobContractResponder {
  static String _collectionFor(String vaultId) => '${vaultId}_blobs';

  RhyoliteBlobResponder({required IBlobClient client}) : _client = client;

  final IBlobClient _client;

  @override
  Future<BulkUploadBlobResponse> upload(
    Stream<BlobChunk> chunks, {
    RpcContext? context,
  }) async {
    // Workaround: rpc_dart client stream over WebSocket (dart2js) delivers
    // each message twice. Deduplicate the stream before processing.
    final deduped = _deduplicateChunks(chunks);
    final queue = StreamQueue<BlobChunk>(deduped);

    if (!await queue.hasNext) {
      return const BulkUploadBlobResponse(blobIds: []);
    }

    final first = await queue.next;
    final vaultId = first.vaultId;
    final blobId = first.blobId;

    if (blobId == null || vaultId == null) {
      throw StateError('First chunk must carry blobId and vaultId');
    }

    context?.log.info('upload vault=$vaultId blobId=${blobId.substring(0, 8)}... size=${first.totalLength}B');

    final blobIds = <String>[];
    var currentFirst = first;
    var currentBlobId = blobId;

    while (true) {
      // Cooperative cancellation between blobs in the batch — the
      // client's x-client-cancelled frame populates this token. Already
      // persisted blobs stay (content-addressed = idempotent).
      context?.cancellationToken?.throwIfCancelled();
      final result = await _client.putBytes(
        collection: _collectionFor(vaultId),
        id: currentBlobId,
        bytes: _drainBlob(currentFirst, queue),
        length: currentFirst.totalLength,
      );
      blobIds.add(result.descriptor.id);

      if (!await queue.hasNext) break;
      currentFirst = await queue.next;
      if (currentFirst.blobId == null) {
        throw StateError('First chunk of each blob must carry blobId');
      }
      currentBlobId = currentFirst.blobId!;
    }

    return BulkUploadBlobResponse(blobIds: blobIds);
  }

  @override
  Future<BulkExistsBlobResponse> bulkExists(
    BulkExistsBlobRequest request, {
    RpcContext? context,
  }) async {
    if (request.blobIds.isEmpty) {
      return const BulkExistsBlobResponse(presentIds: []);
    }
    final collection = _collectionFor(request.vaultId);
    final response = await _client.bulkHeadBlob(
      BulkHeadBlobRequest(
        items: request.blobIds
            .map((id) => HeadBlobRequest(collection: collection, id: id))
            .toList(),
      ),
      context: context,
    );
    final present = response.items
        .where((r) => r.descriptor != null)
        .map((r) => r.id)
        .toList();
    context?.log.info(
      'bulkExists vault=${request.vaultId} '
      'requested=${request.blobIds.length} present=${present.length}',
    );
    return BulkExistsBlobResponse(presentIds: present);
  }

  /// Deduplicates a chunk stream where every message is delivered twice.
  /// Tracks (blobId, offset) pairs and drops already-seen chunks.
  Stream<BlobChunk> _deduplicateChunks(Stream<BlobChunk> source) async* {
    String? currentBlobId;
    int lastOffset = -1;

    await for (final chunk in source) {
      final id = chunk.blobId ?? currentBlobId;
      if (id == currentBlobId && chunk.offset <= lastOffset) {
        // Duplicate — skip.
        continue;
      }
      if (chunk.blobId != null) currentBlobId = chunk.blobId;
      lastOffset = chunk.offset;
      yield chunk;
    }
  }

  Stream<Uint8List> _drainBlob(
    BlobChunk first,
    StreamQueue<BlobChunk> queue,
  ) async* {
    yield first.bytes;
    if (first.last) return;
    while (await queue.hasNext) {
      final chunk = await queue.next;
      yield chunk.bytes;
      if (chunk.last) return;
    }
  }

  @override
  Stream<BlobChunk> download(
    BulkDownloadBlobRequest request, {
    RpcContext? context,
  }) async* {
    context?.log.info(
      'download vault=${request.vaultId} '
      'blobs=[${request.blobIds.map((id) => id.substring(0, 8)).join(', ')}...]',
    );
    final collection = _collectionFor(request.vaultId);
    final bulkRequest = BulkGetBlobRequest(
      items: request.blobIds
          .map((id) => GetBlobRequest(collection: collection, id: id))
          .toList(),
    );
    try {
      await for (final frame in _client.bulkGetBlob(
        bulkRequest,
        context: context,
      )) {
        yield BlobChunk(
          bytes: frame.frame.bytes,
          offset: frame.frame.offset,
          last: frame.frame.last,
          blobId: frame.frame.descriptor != null ? frame.id : null,
        );
      }
    } catch (e) {
      context?.log.error('download failed vault=${request.vaultId}: $e');
      rethrow;
    }
  }

  @override
  Future<BulkDeleteBlobsResponse> bulkDelete(
    BulkDeleteBlobsRequest request, {
    RpcContext? context,
  }) async {
    if (request.blobIds.isEmpty) {
      return const BulkDeleteBlobsResponse(deleted: 0);
    }
    final collection = _collectionFor(request.vaultId);
    final response = await _client.bulkDeleteBlob(
      BulkDeleteBlobRequest(
        items: request.blobIds
            .map((id) => DeleteBlobRequest(collection: collection, id: id))
            .toList(),
      ),
      context: context,
    );
    final deleted = response.items.where((r) => r.deleted).length;
    context?.log.info(
      'bulkDelete vault=${request.vaultId} '
      'requested=${request.blobIds.length} deleted=$deleted',
    );
    return BulkDeleteBlobsResponse(deleted: deleted);
  }
}
