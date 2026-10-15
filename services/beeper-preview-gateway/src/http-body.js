// Cancel a rejected body without destroying the socket before its 413 is written.
export function httpRequestBody(request) {
  let detach = () => {};
  return new ReadableStream({
    start(controller) {
      const onData = chunk => {
        controller.enqueue(new Uint8Array(chunk));
        if (controller.desiredSize <= 0) request.pause();
      };
      const onEnd = () => { detach(); controller.close(); };
      const onError = error => { detach(); controller.error(error); };
      detach = () => {
        request.off('data', onData);
        request.off('end', onEnd);
        request.off('error', onError);
      };
      request.on('data', onData);
      request.once('end', onEnd);
      request.once('error', onError);
    },
    pull() { request.resume(); },
    cancel() {
      request.pause();
      detach();
      request.once('error', () => {});
    },
  });
}
