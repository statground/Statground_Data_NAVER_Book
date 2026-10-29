#!/usr/bin/env python3
"""Finite fixtures only. No configured endpoints, credentials or DB are used."""
import copy
import datetime as dt
import hashlib
import importlib.util
import json
import io
import os
from pathlib import Path
import stat
import tempfile
import types
import unittest
from unittest import mock
import uuid

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("host_observation", HERE / "read_book_host_publication.py")
H = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(H)
KST = dt.timezone(dt.timedelta(hours=9))
ENDPOINTS = ("clickhouse-s1-r1", "clickhouse-s1-r2", "clickhouse-s2-r1", "clickhouse-s2-r2")
SNAPSHOT = "11111111-1111-4111-8111-111111111111"
CONTENT = "a" * 64
MANIFEST = "b" * 64


def exact_uint(value, _label):
    H.need(type(value) is int or type(value) is str and value.isdigit())
    number = int(value)
    H.need(0 <= number < 1 << 64)
    return number


def exact_uuid(value, _label):
    result = uuid.UUID(value)
    H.need(result.int > 0)
    return result


def fixture():
    completed = dt.datetime.now(KST).replace(microsecond=0) - dt.timedelta(minutes=1)
    state = types.SimpleNamespace(started=completed-dt.timedelta(seconds=10), completed=completed,
        generation=completed-dt.timedelta(seconds=5),
        stats={"rows":2,"identities":2,"content_sha256":CONTENT,"policy_revision":7,
               "fingerprint_sum":12,"fingerprint_xor":8,"max_ingested_at":"fixed"},
        target={"rows":2}, complete=True, content=True, active_calls=0, post_change=False,
        post_error=False, calls=[])
    def active(manifest=MANIFEST, snapshot=SNAPSHOT, count="1"):
        return [{"endpoint":endpoint,"active_count":count,
                 "snapshot_uuid":snapshot,"source_manifest_sha256":manifest} for endpoint in ENDPOINTS]
    state.active = active()
    state.active_builder = active

    class Client:
        def __init__(self, *_a, **_kw): pass
        def rows(self, sql):
            H.need(sql == H.ACTIVE_SQL)
            state.active_calls += 1
            rows = copy.deepcopy(state.active)
            if state.post_change and state.active_calls == 2:
                rows[0]["source_manifest_sha256"] = "c" * 64
            return rows

    class Publisher:
        def __init__(self, *_a): pass
        def _preflight(self, *, read_only):
            H.need(read_only is True)
            state.calls.append("readonly_preflight")
            return (state.started,state.completed,state.generation,copy.deepcopy(state.stats),
                    copy.deepcopy(state.target),(state.started,state.completed,3,2))
        def heartbeat(self, *_a):
            return {"snapshot_uuid":SNAPSHOT,"source_manifest_sha256":MANIFEST,"content_sha256":CONTENT,
                    "source_row_count":2,"source_fingerprint_sum":12,"source_fingerprint_xor":8,
                    "policy_authority_revision":7}
        def _snapshot_complete(self, _h):
            state.calls.append("complete_full4")
            return state.complete
        def _snapshot_rows_exact(self, _h):
            state.calls.append("rows_and_ordered_content_full4")
            return state.content
        def assert_publication_inputs(self, *_a):
            state.calls.append("input_authority_postcheck")
            H.need(not state.post_error,"authority_changed")

    sdk = types.SimpleNamespace(SERVICES={"webr":object()},ClickHouseHTTP=Client,Publisher=Publisher,
        COORDINATOR_TIMEOUT_SECONDS=2100,KST=KST,SOURCE_DDL_ENDPOINTS=ENDPOINTS,
        exact_uint=exact_uint,exact_uuid=exact_uuid,DIGEST=H.re.compile(r"[a-f0-9]{64}"),
        parse_time=dt.datetime.fromisoformat,format_time=lambda value,_tz:value.isoformat())
    config = types.SimpleNamespace(coordinator=object())
    expected = H.preflight_receipt(sdk,state.started,state.completed,state.generation,state.stats,state.target)
    return sdk,config,state,expected


class PublicObservation(unittest.TestCase):
    def test_complete_requires_all_original_postchecks(self):
        sdk,config,state,expected=fixture()
        value=H.observe_public(sdk,config,expected)
        self.assertTrue(value["complete"])
        self.assertEqual(state.calls,["readonly_preflight","complete_full4",
            "rows_and_ordered_content_full4","input_authority_postcheck"])
        self.assertEqual(state.active_calls,2)
        self.assertFalse(value["wire_ack_proved"] or value["host_request_sent"])

    def test_positive_absence_is_pending(self):
        sdk,config,state,expected=fixture()
        state.active=state.active_builder(None,None,"0")
        value=H.observe_public(sdk,config,expected)
        self.assertEqual(value["status"],"pending")
        self.assertFalse(value["complete"])
        self.assertEqual(state.calls,["readonly_preflight","input_authority_postcheck"])

    def test_complete_other_manifest_is_pending_not_newest_adoption(self):
        sdk,config,state,expected=fixture()
        state.active=state.active_builder("c"*64)
        value=H.observe_public(sdk,config,expected)
        self.assertEqual(value["status"],"pending")
        self.assertEqual(value["source_manifest_sha256"],MANIFEST)

    def test_partial_duplicate_extra_and_empty_are_not_absence(self):
        for shape in ("partial","duplicate","extra","empty"):
            with self.subTest(shape=shape):
                sdk,config,state,expected=fixture()
                if shape=="partial":state.active.pop()
                if shape=="duplicate":state.active[3]=copy.deepcopy(state.active[0])
                if shape=="extra":state.active.append(copy.deepcopy(state.active[0]))
                if shape=="empty":state.active=[]
                with self.assertRaises(H.Refused):H.observe_public(sdk,config,expected)

    def test_mixed_or_duplicate_active_states_refuse(self):
        for key,value in (("source_manifest_sha256","c"*64),("active_count","2")):
            sdk,config,state,expected=fixture();state.active[0][key]=value
            with self.assertRaises(H.Refused):H.observe_public(sdk,config,expected)

    def test_same_manifest_different_content_identity_refuses(self):
        sdk,config,state,expected=fixture()
        state.active=state.active_builder(snapshot="22222222-2222-4222-8222-222222222222")
        with self.assertRaises(H.Refused):H.observe_public(sdk,config,expected)

    def test_missing_completion_and_content_cannot_be_pending(self):
        for field in ("complete","content"):
            sdk,config,state,expected=fixture();setattr(state,field,False)
            with self.assertRaises(H.Refused):H.observe_public(sdk,config,expected)

    def test_post_authority_and_active_change_refuse(self):
        for field in ("post_error","post_change"):
            sdk,config,state,expected=fixture();setattr(state,field,True)
            with self.assertRaises(H.Refused):H.observe_public(sdk,config,expected)

    def test_every_frozen_preflight_operand_must_match(self):
        for key in ("source_refresh_started_at","source_refresh_success_at","source_generation",
                    "source_rows","raw_target_rows","content_sha256","policy_authority_revision"):
            sdk,config,state,expected=fixture()
            expected[key]=expected[key]+1 if type(expected[key]) is int else "c"*64 if key=="content_sha256" else "2000-01-01T00:00:00+09:00"
            with self.subTest(key=key),self.assertRaises(H.Refused):H.observe_public(sdk,config,expected)

    def test_preflight_returns_exact_ten_fields_without_active_claim(self):
        sdk,config,state,_expected=fixture()
        value=H.observe_public(sdk,config,preflight_only=True)
        self.assertEqual(set(value),H.PREFLIGHT_KEYS)
        self.assertIs(value["database_writes"],False)
        self.assertEqual(state.active_calls,0)

    def test_boolean_counts_extra_keys_and_bad_digests_refuse(self):
        for key,value in (("source_rows",True),("content_sha256","A"*64),("extra",1)):
            sdk,_config,_state,expected=fixture();expected[key]=value
            with self.subTest(key=key),self.assertRaises(H.Refused):H.expected_receipt(sdk,expected)

    def test_null_and_noncanonical_active_identity_refuse(self):
        sdk,_config,state,_expected=fixture()
        for value in (None,"bad",str(uuid.UUID(int=0))):
            state.active=state.active_builder(snapshot=value)
            with self.subTest(value=value),self.assertRaises((H.Refused,ValueError,AttributeError)):
                H.active_inventory(sdk,sdk.ClickHouseHTTP())


class ClosedInputAndOutput(unittest.TestCase):
    def test_duplicate_and_nonfinite_json_refuse(self):
        for raw in ('{"a":1,"a":2}','{"a":NaN}','{"a":Infinity}'):
            with self.subTest(raw=raw),self.assertRaises(H.Refused):H.loads(raw)

    def test_result_has_closed_twelve_fields(self):
        value=H.result("legacy","compatibility_unknown","legacy_contract_not_observed")
        self.assertEqual(len(value),12)
        self.assertFalse(value["complete"] or value["wire_ack_proved"] or value["host_request_sent"])
        self.assertNotIn("error",value)

    def test_main_discards_arbitrary_error_text(self):
        output=io.StringIO()
        with mock.patch.object(H,"run",side_effect=ValueError("PRIVATE_CANARY_DO_NOT_OUTPUT")),mock.patch("sys.stdout",output):
            code=H.main(["--contract","legacy","--config","/fixture/config","--sql-source-root","/fixture/sql"])
        self.assertEqual(code,1)
        self.assertNotIn("PRIVATE_CANARY",output.getvalue())
        self.assertEqual(json.loads(output.getvalue())["reason"],"observation_failed")

    def test_main_exit_status_is_typed(self):
        for status,reason,expected in (("complete","observed_complete",0),
                ("pending","awaiting_authoritative_host_publication",2),
                ("compatibility_unknown","legacy_contract_not_observed",2),
                ("refused","readonly_request_refused",1)):
            contract="public-v2" if status=="pending" else "legacy"
            with self.subTest(status=status),mock.patch.object(H,"run",return_value=H.result(contract,status,reason)),mock.patch("sys.stdout",io.StringIO()):
                self.assertEqual(H.main(["--contract","legacy","--config","/fixture/config","--sql-source-root","/fixture/sql"]),expected)

    def test_status_reason_and_contract_cannot_conflict(self):
        for contract,status,reason in (("public-v2","complete","budget_exhausted"),
                ("legacy","pending","awaiting_authoritative_host_publication"),
                ("public-v2","compatibility_unknown","legacy_contract_not_observed"),
                ("legacy","refused","observed_complete")):
            with self.subTest(status=status),self.assertRaises(H.Refused):H.result(contract,status,reason)

    def test_held_pin_metadata_symlink_and_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"owned";path.write_bytes(b"safe");path.chmod(0o600)
            held=H.Held(path,private=True,expected=hashlib.sha256(b"safe").hexdigest())
            try:
                path.write_bytes(b"drift")
                with self.assertRaises(H.Refused):held.same()
            finally:held.close()
            path.chmod(0o644)
            with self.assertRaises(H.Refused):H.Held(path,private=True)
            link=Path(directory)/"link";link.symlink_to(path)
            with self.assertRaises(H.Refused):H.Held(link)

    def test_earliest_cleanup_is_sticky_and_clipped(self):
        with mock.patch.object(H.time,"monotonic",side_effect=[10,12,18]):
            window=H.Window(8)
            self.assertEqual(window.cleanup(),18)
            self.assertEqual(window.cleanup(),18)
            with self.assertRaises(H.Refused):window.check()

    def test_wire_reserves_full_request_and_cleanup_before_attempt(self):
        window=H.Window(10)
        with self.assertRaises(H.Refused):
            with window.wire(5):self.fail("unreserved wire entered")

    def test_earlier_alarm_and_expired_deadline_are_never_renewed(self):
        with mock.patch.object(H.time,"monotonic",return_value=100),mock.patch.object(H.signal,"getitimer",return_value=(5,0)):
            window=H.Window(600)
            self.assertEqual(window.end,105)
        window.armed=True
        with mock.patch.object(H.time,"monotonic",return_value=106),mock.patch.object(H.signal,"setitimer") as alarm:
            with self.assertRaises(H.Refused):window.arm(105)
            alarm.assert_not_called()


class OriginalPinnedSDK(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Root supplies a pinned source checkout; this is never a configured
        # DB or credential path and importing the source has no native effect.
        root=os.environ.get("BOOK_HOST_TEST_SQL_ROOT")
        if not root:
            raise RuntimeError("BOOK_HOST_TEST_SQL_ROOT must point to the pinned source checkout")
        cls.source=H.Held(Path(root).resolve()/H.RELATIVE/H.SDK_NAME,expected=H.SDK_SHA256)
        cls.sdk=H.module_from_held(cls.source,"book_host_fixture_sdk")

    @classmethod
    def tearDownClass(cls):cls.source.close()

    def setUp(self):
        self.window=H.Window(60)
        self.Safe=H.readonly_client_class(self.sdk,[self.source],self.window)
        self.endpoint=self.sdk.Endpoint("https://fixture.invalid/","webr_book_serving_publisher","FAKE_ONLY")

    def test_no_arbitrary_sql_serial_exec_or_insert(self):
        client=self.Safe(self.endpoint)
        for action in (lambda:client.rows("SELECT 1"),lambda:client.rows("SELECT generateSerialID('x')"),
                       lambda:client.exec("SYSTEM SYNC REPLICA x"),lambda:client.insert_json("a.b",[],"")):
            with self.assertRaises(H.Refused):action()
        self.assertEqual(self.window.requests,0)

    def test_no_default_profile_or_timeout_increase(self):
        with self.assertRaises(H.Refused):self.Safe(self.endpoint,read_only=False)
        with self.assertRaises(H.Refused):self.Safe(self.endpoint,timeout=2101)

    def test_fixed_http_post_is_explicit_readonly_two_and_closes(self):
        rows=[{"endpoint":x,"active_count":"0","snapshot_uuid":None,"source_manifest_sha256":None} for x in ENDPOINTS]
        body=("\n".join(json.dumps(x) for x in rows)+"\n").encode();seen=[]
        class Response:
            status=200
            def read(self,_maximum):return body
            def close(self):seen.append("response_closed")
        class Connection:
            def __init__(self,*_a,**_kw):pass
            def request(self,method,target,body,headers):seen.append((method,target,body))
            def getresponse(self):return Response()
            def close(self):seen.append("connection_closed")
        with mock.patch.object(self.sdk.http.client,"HTTPSConnection",Connection):
            value=self.Safe(self.endpoint).rows(H.ACTIVE_SQL)
        self.assertEqual(value,rows)
        self.assertEqual(seen[0][0],"POST")
        self.assertIn("readonly=2",seen[0][1])
        self.assertIn("timeout_overflow_mode_leaf=throw",seen[0][1])
        for setting in H.OVERFLOW_MODES:
            self.assertIn(setting+"=throw",seen[0][1])
        self.assertEqual(seen[0][2],(H.ACTIVE_SQL+" FORMAT JSONEachRow").encode())
        self.assertIn("response_closed",seen);self.assertIn("connection_closed",seen)

    def test_real_snapshot_completion_requires_four_matching_rows(self):
        heartbeat={"snapshot_uuid":SNAPSHOT,"content_sha256":CONTENT,"policy_authority_revision":7,
                   "source_row_count":2,"source_fingerprint_sum":12,"source_fingerprint_xor":8}
        expected={"snapshot_uuid":SNAPSHOT,"content_sha256":CONTENT,"policy_authority_revision":"7",
                  "row_count":"2","fingerprint_sum":"12","fingerprint_xor":"8"}
        client=types.SimpleNamespace(rows=lambda *_a,**_kw:[dict(expected) for _ in range(3)])
        publisher=self.sdk.Publisher(client,None,self.sdk.SERVICES["webr"])
        self.assertFalse(publisher._snapshot_complete(heartbeat))
        client.rows=lambda *_a,**_kw:[dict(expected) for _ in range(4)]
        self.assertTrue(publisher._snapshot_complete(heartbeat))

    def test_real_source_declares_full_ordered_content_checks(self):
        import ast
        tree=ast.parse(self.source.raw)
        functions={node.name:node for node in ast.walk(tree) if isinstance(node,ast.FunctionDef)}
        attrs=lambda name:{node.func.attr for node in ast.walk(functions[name]) if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute)}
        self.assertIn("_snapshot_content_exact",attrs("_snapshot_rows_exact"))
        self.assertIn("stream_ordered_identity_content_sha256",attrs("_snapshot_content_exact"))
        self.assertEqual(self.sdk.EXPECTED_ENDPOINTS,4)
        self.assertEqual(self.sdk.ENDPOINT_STREAM_ATTEMPTS,3)

    def test_actual_sdk_ordered_hash_complete_tampered_and_partial(self):
        records=[{"identity":"01","row_sha":"1"*64},{"identity":"02","row_sha":"2"*64}]
        digest=hashlib.sha256(b"statground.book-serving-content.v2\0webr\0"+b"7\n")
        for row in records:digest.update(row["row_sha"].encode()+b"\n")
        heartbeat={"snapshot_uuid":SNAPSHOT,"content_sha256":digest.hexdigest(),
                   "policy_authority_revision":7,"source_row_count":2}
        for mode in ("complete","tampered","partial","truncated"):
            with self.subTest(mode=mode):
                window=H.Window(600)
                Safe=H.readonly_client_class(self.sdk,[self.source],window)
                class Sock:
                    def __init__(self,raw):self.raw=raw
                    def makefile(self,_mode):return io.BytesIO(self.raw)
                class Connection:
                    def __init__(self,*_a,**_kw):self.sql=b""
                    def request(self,_method,_target,body,_headers=None,headers=None):self.sql=body
                    def getresponse(self):
                        if b" AS identity," not in self.sql:
                            rows=[{"endpoint":x} for x in ENDPOINTS]
                        else:
                            rows=copy.deepcopy(records)
                            if mode=="tampered":rows[1]["row_sha"]="3"*64
                            if mode=="partial":rows.pop()
                        body=("\n".join(json.dumps(x) for x in rows)+"\n").encode()
                        declared=len(body)+(1 if mode=="truncated" and b" AS identity," in self.sql else 0)
                        response=H.CompleteHTTPResponse(Sock(b"HTTP/1.1 200 OK\r\nContent-Length: "+str(declared).encode()+b"\r\n\r\n"+body))
                        response.begin();return response
                    def close(self):pass
                with mock.patch.object(self.sdk.http.client,"HTTPSConnection",Connection):
                    publisher=self.sdk.Publisher(Safe(self.endpoint,timeout=45),None,self.sdk.SERVICES["webr"])
                    if mode in ("partial","truncated"):
                        with self.assertRaises((H.Refused,self.sdk.PublishError)):
                            publisher._snapshot_content_exact(heartbeat)
                    else:self.assertEqual(publisher._snapshot_content_exact(heartbeat),mode=="complete")

    def test_real_raw_target_identity_stream_is_counted_and_framed(self):
        for mode in ("complete","truncated"):
            with self.subTest(mode=mode):
                window=H.Window(600);Safe=H.readonly_client_class(self.sdk,[self.source],window)
                class Sock:
                    def __init__(self,raw):self.raw=raw
                    def makefile(self,_mode):return io.BytesIO(self.raw)
                class Connection:
                    def __init__(self,*_a,**_kw):self.sql=b""
                    def request(self,_method,_target,body,headers):self.sql=body
                    def getresponse(self):
                        streamed=b" AS identity " in self.sql
                        if streamed:rows=[{"identity":"01"},{"identity":"02"}]
                        elif b"GROUP BY hostName()" in self.sql:
                            rows=[{"endpoint":name,"shard_name":str(index//2),"replica_name":str(index%2),
                                   "rows":"1","fingerprint_sum":"6","fingerprint_xor":"4" if index<2 else "12"}
                                  for index,name in enumerate(ENDPOINTS)]
                        else:rows=[{"rows":"2","fingerprint_sum":"12","fingerprint_xor":"8"}]
                        body=("\n".join(json.dumps(x) for x in rows)+"\n").encode()
                        count=len(body)+(1 if mode=="truncated" and streamed else 0)
                        response=H.CompleteHTTPResponse(Sock(b"HTTP/1.1 200 OK\r\nContent-Length: "+str(count).encode()+b"\r\n\r\n"+body))
                        response.begin();return response
                    def close(self):pass
                with mock.patch.object(self.sdk.http.client,"HTTPSConnection",Connection):
                    client=Safe(self.endpoint,timeout=45)
                    if mode=="truncated":
                        with self.assertRaises(H.Refused):
                            self.sdk.raw_target_stats(client,self.sdk.SERVICES["webr"],dt.datetime.now(KST),2)
                    else:
                        value=self.sdk.raw_target_stats(client,self.sdk.SERVICES["webr"],dt.datetime.now(KST),2)
                        self.assertEqual(value["identities"],2)
                        self.assertEqual(window.requests,3)


class LegacyProtocol(unittest.TestCase):
    def fake(self,marker=True,stale=False):
        success=dt.datetime.now(KST).replace(tzinfo=None,microsecond=0)-dt.timedelta(hours=14 if stale else 1)
        value={"refresh_success_time":success.isoformat(),"generation":success.isoformat(),
               "row_count":2,"fingerprint_sum":12,"fingerprint_xor":8,"marker_present":marker}
        refresh=types.SimpleNamespace(last_success_time=success)
        signature=types.SimpleNamespace(row_count=2,fingerprint_sum=12,fingerprint_xor=8)
        state=types.SimpleNamespace(pointer=True,refresh=refresh,marker=marker)
        class Publisher:
            def __init__(self,_c):pass
            def validate_health(self):pass
            def inspect(self):return value
            def refresh_evidence(self):return state.refresh
            def select_generation(self,_r):return success
            def validate_generation(self,*_a):return signature
            def verify_marker(self,_p):return state.marker
            def verify_pointer(self,_p):H.need(state.pointer,"authority_changed")
        legacy=types.SimpleNamespace(Publisher=Publisher,parse_time=dt.datetime.fromisoformat,
            format_time=lambda x:x.isoformat(),marker_payload=lambda *_a:{})
        return legacy,state

    def test_legacy_missing_marker_is_not_public_v2_complete(self):
        legacy,_state=self.fake(False)
        value=H.observe_legacy(types.SimpleNamespace(KST=KST),legacy,object())
        self.assertEqual(value["status"],"compatibility_unknown")
        self.assertFalse(value["complete"])
        self.assertIsNone(value["source_manifest_sha256"])

    def test_legacy_positive_requires_its_own_marker_and_pointer(self):
        legacy,state=self.fake()
        self.assertTrue(H.observe_legacy(types.SimpleNamespace(KST=KST),legacy,object())["complete"])
        state.pointer=False
        with self.assertRaises(H.Refused):H.observe_legacy(types.SimpleNamespace(KST=KST),legacy,object())

    def test_legacy_stale_or_changed_refresh_refuses(self):
        legacy,_state=self.fake(stale=True)
        with self.assertRaises(H.Refused):H.observe_legacy(types.SimpleNamespace(KST=KST),legacy,object())
        legacy,state=self.fake();state.refresh=types.SimpleNamespace(last_success_time=dt.datetime(2000,1,1))
        with self.assertRaises(H.Refused):H.observe_legacy(types.SimpleNamespace(KST=KST),legacy,object())


class OriginalLegacySource(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=os.environ.get("BOOK_HOST_TEST_LEGACY_SOURCE")
        if not path:
            raise RuntimeError("BOOK_HOST_TEST_LEGACY_SOURCE must identify exact old27cb source")
        cls.held=H.Held(Path(path).resolve(),expected=H.LEGACY_SHA256)
        cls.legacy=H.module_from_held(cls.held,"book_host_fixture_legacy")

    @classmethod
    def tearDownClass(cls):cls.held.close()

    def protocol(self,marker=True,pointer=True):
        legacy=self.legacy
        success=dt.datetime.now(KST).replace(tzinfo=None,microsecond=0)-dt.timedelta(minutes=1)
        stamp=legacy.format_time(success)
        refresh={"endpoint":ENDPOINTS[0],"status":"Scheduled","last_success_time":stamp,
                 "last_refresh_time":stamp,"read_rows":2,"written_rows":2,"has_exception":0}
        health=[]
        for table in ("naver_r_book_catalog_local","naver_r_book_catalog_generation_local"):
            for index,endpoint in enumerate(ENDPOINTS):
                health.append({"endpoint":endpoint,"table":table,
                    "zookeeper_path":"/global" if "generation" in table else "/shard"+str(index//2),
                    "replica_name":str(index%2),"is_readonly":0,"is_session_expired":0,"queue_size":0,
                    "future_parts":0,"parts_to_check":0,"lost_part_count":0,"log_pointer":1,"log_max_index":1,
                    "active_replicas":4 if "generation" in table else 2,
                    "total_replicas":4 if "generation" in table else 2})
        global_row={"row_count":2,"isbn_count":2,"canonical_isbn_count":2,"fingerprint_sum":12,"fingerprint_xor":8}
        physical=[{"shard_name":str(i),"endpoint_count":2,"replica_count":2,"signature_versions":1,
                   "row_count":1,"isbn_count":1,"canonical_isbn_count":1,
                   "fingerprint_sum":6,"fingerprint_xor":x} for i,x in enumerate((4,12))]
        evidence=legacy.one_refresh_evidence([refresh])
        signature=legacy.signature_from_row(success,global_row)
        payload=legacy.marker_payload(evidence,signature,success)
        state=types.SimpleNamespace(calls=[],marker=marker,pointer=pointer,health=health,refresh_calls=0,drift_final=False)
        class Client:
            def query_json(self,sql,_params=None):
                state.calls.append(sql)
                if sql==legacy.REPLICA_HEALTH_SQL:return copy.deepcopy(health)
                if sql==legacy.REFRESH_STATE_SQL:
                    state.refresh_calls+=1
                    result=dict(refresh)
                    if state.drift_final and state.refresh_calls>1:
                        result["last_success_time"]=legacy.format_time(success+dt.timedelta(seconds=1))
                    return [result]
                if sql==legacy.CANDIDATE_GENERATIONS_SQL:return [{"generation":stamp}]
                if sql==legacy.GLOBAL_SIGNATURE_SQL:return [dict(global_row)]
                if sql==legacy.PHYSICAL_SIGNATURE_SQL:return copy.deepcopy(physical)
                if sql==legacy.MARKER_ROWS_SQL:return [{"endpoint":endpoint,**payload} for endpoint in ENDPOINTS] if state.marker else []
                if sql==legacy.POINTER_SQL:return [dict(payload)] if state.pointer else []
                raise AssertionError("unlisted legacy SQL")
        return Client(),state

    def test_exact_old_source_full_legacy_marker_pointer_positive(self):
        client,state=self.protocol()
        value=H.observe_legacy(types.SimpleNamespace(KST=KST),self.legacy,client)
        self.assertTrue(value["complete"])
        self.assertIn(self.legacy.MARKER_ROWS_SQL,state.calls)
        self.assertIn(self.legacy.POINTER_SQL,state.calls)
        self.assertTrue(all(sql.lstrip().startswith(("SELECT","WITH")) for sql in state.calls))

    def test_exact_old_source_marker_absence_remains_unknown(self):
        client,_state=self.protocol(marker=False)
        value=H.observe_legacy(types.SimpleNamespace(KST=KST),self.legacy,client)
        self.assertEqual(value["status"],"compatibility_unknown")

    def test_exact_old_source_pointer_mismatch_refuses(self):
        client,_state=self.protocol(pointer=False)
        with self.assertRaises(self.legacy.PublishError):
            H.observe_legacy(types.SimpleNamespace(KST=KST),self.legacy,client)

    def test_legacy_current_refresh_recheck_follows_bulk_marker_reads(self):
        client,state=self.protocol();state.drift_final=True
        with self.assertRaises(H.Refused):H.observe_legacy(types.SimpleNamespace(KST=KST),self.legacy,client)
        self.assertEqual(state.calls[-1],self.legacy.REFRESH_STATE_SQL)

    def test_legacy_healthy_counts_with_duplicate_or_wrong_hosts_refuse(self):
        _client,state=self.protocol()
        sdk=types.SimpleNamespace(SOURCE_DDL_ENDPOINTS=ENDPOINTS)
        for kind in ("duplicate","wrong"):
            rows=copy.deepcopy(state.health)
            rows[3]["endpoint"]=rows[0]["endpoint"] if kind=="duplicate" else "wrong-host"
            with self.subTest(kind=kind),self.assertRaises(H.Refused):
                H.legacy_inventory(sdk,self.legacy,self.legacy.REPLICA_HEALTH_SQL,rows)


class HTTPFraming(unittest.TestCase):
    def response(self,headers,body):
        class Sock:
            def makefile(self,_mode):return io.BytesIO(b"HTTP/1.1 200 OK\r\n"+headers+b"\r\n"+body)
        response=H.CompleteHTTPResponse(Sock());response.begin();return response

    def test_complete_length_and_chunked_bodies(self):
        for headers,body in ((b"Content-Length: 3\r\n",b"{}\n"),
                (b"Content-Length: 0\r\n",b""),
                (b"Transfer-Encoding: chunked\r\n",b"3\r\n{}\n\r\n0\r\n\r\n")):
            response=self.response(headers,body)
            try:self.assertIn(response.read(20),(b"{}\n",b""))
            finally:response.close()

    def test_valid_json_prefix_with_truncated_length_is_refused(self):
        response=self.response(b"Content-Length: 4\r\n",b"{}\n")
        try:
            with self.assertRaises(H.Refused):response.read(20)
        finally:response.close()

    def test_complete_length_keepalive_line_need_not_be_closed(self):
        response=self.response(b"Content-Length: 3\r\n",b"{}\n")
        try:
            self.assertEqual(response.readline(20),b"{}\n")
            self.assertEqual(response.readline(20),b"")
            response.verify_end()
        finally:response.close()

    def test_chunk_size_is_not_an_extra_ordinary_response_cap(self):
        response=self.response(b"Transfer-Encoding: chunked\r\n",b"4000001\r\n")
        try:self.assertEqual(response._get_chunk_left(),67108865)
        finally:response.close()

    def test_duplicate_ambiguous_unframed_and_exception_headers_refuse(self):
        headers=(b"Content-Length: 3\r\nContent-Length: 3\r\n",
                 b"Content-Length: 3\r\nTransfer-Encoding: chunked\r\n",
                 b"Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n",b"",
                 b"Content-Length: 3\r\nX-ClickHouse-Exception-Code: 497\r\n",
                 b"Content-Length: 3\r\nX-ClickHouse-Exception-Code: 0\r\nX-ClickHouse-Exception-Code: 0\r\n")
        for header in headers:
            with self.subTest(header=header),self.assertRaises(H.Refused):self.response(header,b"{}\n")

    def test_chunk_terminators_extensions_and_trailers_refuse(self):
        bodies=(b"3\r\n{}\n\r\n0\r\n",b"3\r\n{}\n\r\n0\r\nX: 1\r\n\r\n",
                b"3;x=y\r\n{}\n\r\n0\r\n\r\n",b"3\r\n{}\nXX0\r\n\r\n")
        for body in bodies:
            response=self.response(b"Transfer-Encoding: chunked\r\n",body)
            try:
                with self.subTest(body=body),self.assertRaises((H.Refused,H.http.client.IncompleteRead)):
                    response.read(20)
            finally:response.close()


if __name__ == "__main__":
    unittest.main()
