from mock import sentinel, create_autospec, patch, call, Mock
from pymongo.collection import Collection

from argus import Argus
from argus.argus import ArgusLibraryBinding
from argus.store.bson_store import BSONStore


def test_enable_sharding():
    argus_lib = create_autospec(ArgusLibraryBinding)
    argus_lib.argus = create_autospec(Argus)
    with patch("argus.store.bson_store.enable_sharding", autospec=True) as enable_sharding:
        argus_lib.get_top_level_collection.return_value.database.create_collection.__name__ = "some_name"
        argus_lib.get_top_level_collection.return_value.database.collection_names.__name__ = "some_name"
        bsons = BSONStore(argus_lib)
        bsons.enable_sharding()
        # Check we always set the sharding to be hashed.
        assert enable_sharding.call_args_list == [call(argus_lib.argus, argus_lib.get_name(), hashed=True, key="_id")]


def test_find():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    collection.find.return_value = (doc for doc in [sentinel.document])
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)

    assert list(bsons.find(sentinel.filter)) == [sentinel.document]
    assert collection.find.call_count == 1
    assert collection.find.call_args_list == [call(sentinel.filter)]


def test_find_one():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    collection.find_one.return_value = sentinel.document
    argus_lib.get_top_level_collection.return_value = collection

    ms = BSONStore(argus_lib)

    assert ms.find_one(sentinel.filter) == sentinel.document
    assert collection.find_one.call_count == 1
    assert collection.find_one.call_args_list == [call(sentinel.filter)]


def test_insert_one():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.insert_one(sentinel.document)

    assert argus_lib.check_quota.call_count == 1
    assert collection.insert_one.call_count == 1
    assert collection.insert_one.call_args_list == [call(sentinel.document)]


def test_insert_many():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.insert_many(sentinel.documents)

    assert argus_lib.check_quota.call_count == 1
    assert collection.insert_many.call_count == 1
    assert collection.insert_many.call_args_list == [call(sentinel.documents)]


def test_replace_one():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.replace_one(sentinel.filter, sentinel.replacement)

    assert argus_lib.check_quota.call_count == 1
    assert collection.replace_one.call_count == 1
    assert collection.replace_one.call_args_list == [call(sentinel.filter, sentinel.replacement)]


def test_update_one():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.update_one(sentinel.filter, sentinel.replacement)

    assert argus_lib.check_quota.call_count == 1
    assert collection.update_one.call_count == 1
    assert collection.update_one.call_args_list == [call(sentinel.filter, sentinel.replacement)]


def test_update_many():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.update_many(sentinel.filter, sentinel.replacements)

    assert argus_lib.check_quota.call_count == 1
    assert collection.update_many.call_count == 1
    assert collection.update_many.call_args_list == [call(sentinel.filter, sentinel.replacements)]


def test_find_one_and_replace():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.find_one_and_replace(sentinel.filter, sentinel.replacement)

    assert argus_lib.check_quota.call_count == 1
    assert collection.find_one_and_replace.call_count == 1
    assert collection.find_one_and_replace.call_args_list == [call(sentinel.filter, sentinel.replacement)]


def test_find_one_and_update():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    ms = BSONStore(argus_lib)
    ms.find_one_and_update(sentinel.filter, sentinel.update)

    assert argus_lib.check_quota.call_count == 1
    assert collection.find_one_and_update.call_count == 1
    assert collection.find_one_and_update.call_args_list == [call(sentinel.filter, sentinel.update)]


def test_find_one_and_delete():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    ms = BSONStore(argus_lib)
    ms.find_one_and_delete(sentinel.filter)

    assert collection.find_one_and_delete.call_count == 1
    assert collection.find_one_and_delete.call_args_list == [call(sentinel.filter)]


def test_bulk_write():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.bulk_write(sentinel.requests)

    assert argus_lib.check_quota.call_count == 1
    assert collection.bulk_write.call_count == 1
    assert collection.bulk_write.call_args_list == [call(sentinel.requests)]


def test_delete_one():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.delete_one(sentinel.filter)

    assert collection.delete_one.call_count == 1
    assert collection.delete_one.call_args_list == [call(sentinel.filter)]


def test_count():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True, count=Mock(), count_documents=Mock())
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.count(sentinel.filter)

    assert collection.count.call_count + collection.count_documents.call_count == 1
    assert collection.count.call_args_list == [
        call(filter=sentinel.filter)
    ] or collection.count_documents.call_args_list == [call(filter=sentinel.filter)]


def test_distinct():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.distinct(sentinel.key)

    assert collection.distinct.call_count == 1
    assert collection.distinct.call_args_list == [call(sentinel.key)]


def test_delete_many():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.delete_many(sentinel.filter)

    assert collection.delete_many.call_count == 1
    assert collection.delete_many.call_args_list == [call(sentinel.filter)]


def test_create_index():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.create_index([(sentinel.path1, sentinel.order1), (sentinel.path2, sentinel.path2)])

    assert collection.create_index.call_count == 1
    assert collection.create_index.call_args_list == [
        call([(sentinel.path1, sentinel.order1), (sentinel.path2, sentinel.path2)])
    ]


def test_drop_index():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.drop_index(sentinel.name)

    assert collection.drop_index.call_count == 1
    assert collection.drop_index.call_args_list == [call(sentinel.name)]


def test_index_information():
    argus_lib = create_autospec(ArgusLibraryBinding, instance=True)
    collection = create_autospec(Collection, instance=True)
    argus_lib.get_top_level_collection.return_value = collection

    bsons = BSONStore(argus_lib)
    bsons.index_information()

    assert collection.index_information.call_count == 1
