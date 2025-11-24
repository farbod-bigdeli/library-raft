# book_service.py

import os
import grpc
from concurrent import futures
import time

from src import book_pb2, book_pb2_grpc
from src.db import SessionLocal, create_db_and_tables
from src import crud

from src import raft_pb2_grpc
from src.raft_node import RaftNode  # your Raft implementation


# Initialize DB tables
create_db_and_tables()


class BookService(book_pb2_grpc.BookServiceServicer):
    def __init__(self, raft_node: RaftNode | None = None):
        self.raft_node = raft_node

    # ------------------------------------------------------------------
    # Helper: ensure Raft is available and replicate the logical operation
    # ------------------------------------------------------------------
    def _replicate_operation(self, operation: str, context):
        """
        Use Raft to replicate a logical operation 'operation'.
        - If this node is leader, it will append+replicate.
        - If this node is follower, it will forward to the leader.
        If replication/forwarding fails, abort the RPC.
        """
        if self.raft_node is None:
            # No Raft; just allow (for debugging / non-Raft mode)
            return

        success = self.raft_node.handle_client_operation(operation)
        if not success:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"Raft replication failed or no leader available on node {self.raft_node.node_id}",
            )

    # -------------------- gRPC methods --------------------

    def AddBook(self, request, context):
        # Build a simple string representation of the operation for Raft log.
        # For the assignment, it's enough to log some 'operation o' string.
        op = (
            f"AddBook(title={request.title},author={request.author},"
            f"isbn={request.isbn})"
        )
        self._replicate_operation(op, context)

        # After Raft says it's committed, we apply the state change locally.
        with SessionLocal() as db:
            book = crud.create_book(
                db,
                request.title,
                request.author,
                request.isbn,
                request.category if request.category else None,
                request.description if request.description else None,
                request.total_copies if request.total_copies > 0 else 1,
            )
            return book_pb2.AddBookResponse(
                book=book_pb2.Book(
                    id=str(book.id),
                    title=book.title,
                    author=book.author,
                    isbn=book.isbn,
                    status=book.status,
                    category=book.category if book.category else "",
                    description=book.description if book.description else "",
                    total_copies=book.total_copies,
                    available_copies=book.available_copies,
                )
            )

    def GetBook(self, request, context):
        with SessionLocal() as db:
            book = crud.get_book(db, request.id)
            if not book:
                context.abort(grpc.StatusCode.NOT_FOUND, "Book not found")

            return book_pb2.GetBookResponse(
                book=book_pb2.Book(
                    id=str(book.id),
                    title=book.title,
                    author=book.author,
                    isbn=book.isbn,
                    status=book.status,
                    category=book.category if book.category else "",
                    description=book.description if book.description else "",
                    total_copies=book.total_copies,
                    available_copies=book.available_copies,
                )
            )

    def ListBooks(self, request, context):
        with SessionLocal() as db:
            books = crud.list_books(db)
            return book_pb2.ListBooksResponse(
                books=[
                    book_pb2.Book(
                        id=str(b.id),
                        title=b.title,
                        author=b.author,
                        isbn=b.isbn,
                        status=b.status,
                        category=b.category if b.category else "",
                        description=b.description if b.description else "",
                        total_copies=b.total_copies,
                        available_copies=b.available_copies,
                    )
                    for b in books
                ]
            )

    def UpdateBookStatus(self, request, context):
        op = f"UpdateBookStatus(id={request.id},status={request.status})"
        self._replicate_operation(op, context)

        with SessionLocal() as db:
            book = crud.update_book_status(db, request.id, request.status)
            if not book:
                context.abort(grpc.StatusCode.NOT_FOUND, "Book not found")

            return book_pb2.UpdateBookStatusResponse(
                book=book_pb2.Book(
                    id=str(book.id),
                    title=book.title,
                    author=book.author,
                    isbn=book.isbn,
                    status=book.status,
                    category=book.category if book.category else "",
                    description=book.description if book.description else "",
                    total_copies=book.total_copies,
                    available_copies=book.available_copies,
                )
            )

    def UpdateAvailableCopies(self, request, context):
        op = f"UpdateAvailableCopies(id={request.id},increment={request.increment})"
        self._replicate_operation(op, context)

        with SessionLocal() as db:
            book = crud.update_available_copies(db, request.id, request.increment)
            if not book:
                context.abort(grpc.StatusCode.NOT_FOUND, "Book not found")

            return book_pb2.UpdateAvailableCopiesResponse(
                book=book_pb2.Book(
                    id=str(book.id),
                    title=book.title,
                    author=book.author,
                    isbn=book.isbn,
                    status=book.status,
                    category=book.category if book.category else "",
                    description=book.description if book.description else "",
                    total_copies=book.total_copies,
                    available_copies=book.available_copies,
                )
            )


def serve():
    # Cluster config for Raft (Docker service names + ports)
    peers = {
        1: "book_service:50053",
        2: "book_service_2:50054",
        3: "book_service_3:50056",
        4: "book_service_4:50057",
        5: "book_service_5:50058",
    }

    # Environment-configured node id and port
    # NODE_ID should be "1", "2" or "3"
    node_id_str = os.getenv("NODE_ID", "1")
    node_id = int(node_id_str)

    port = os.getenv("BOOK_SERVICE_PORT", "50053")

    # gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Raft node for this process
    raft_node = RaftNode(node_id=node_id, peers=peers)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_node, server)

    # BookService, aware of Raft state
    book_service = BookService(raft_node=raft_node)
    book_pb2_grpc.add_BookServiceServicer_to_server(book_service, server)

    server.add_insecure_port(f"[::]:{port}")
    print(
        f"✅ BookService gRPC server running on port {port} "
        f"for node_id={node_id} (initial state={raft_node.state})..."
    )

    server.start()
    # Start Raft background loop after server is up
    raft_node.start()

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down BookService...")
        server.stop(0)


if __name__ == "__main__":
    serve()
