package com.pmi.kafka;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.SerializationFeature;
import static org.junit.jupiter.api.Assertions.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParseAuditLogFunctionTest {

    ParseAuditLogFunction udf = new ParseAuditLogFunction();

    @Test
    void testSimple() throws IOException {

        String hostname = "localhost";
        //String line = "type=SYSCALL msg=audit(1744888492.778:66060): arch=c000003e syscall=59 success=yes exit=0 a0=28ab530 a1=27ccc80 a2=7ffc60710728 a3=0 items=2 ppid=7099 pid=7102 auid=4294967295 uid=0 gid=0 euid=0 suid=0 fsuid=0 egid=0 sgid=0 fsgid=0 tty=(none) ses=4294967295 comm=\"rpm\" exe=\"/usr/bin/rpm\" subj=system_u:system_r:unconfined_service_t:s0 key=\"software_mgmt\"";
        //String line = "type=AVC msg=audit(1754395788.018:20895): apparmor=\"ALLOWED\" operation=\"open\" profile=\"/usr/sbin/sssd\" name=\"/proc/11336/cmdline\" pid=1462 comm=\"sssd_nss\" requested_mask=\"r\" denied_mask=\"r\" fsuid=0 ouid=0\u001dFSUID=\"root\" OUID=\"root\"";
        String line =  "type=USER_END msg=audit(1754395786.670:20894): pid=11333 uid=0 auid=4294967295 ses=4294967295 subj=unconfined msg='op=PAM:session_close grantors=pam_permit,pam_umask,pam_winbind acct=\"testuser1\" exe=\"/usr/sbin/smbd\" hostname=10.144.86.74 addr=10.144.86.74 terminal=smb/3802623126 res=success'\u001dUID=\"root\" AUID=\"unset\"";

        Row result = udf.eval(hostname, line);
        System.out.println(result);

        assertNotNull(result);
        assertNotNull(result.getField(0), "JSON should not be null on success");
        assertNull(result.getField(1), "errorType should be null");
        assertNull(result.getField(2), "errorClass should be null");
        assertNull(result.getField(3), "errorMessage should be null");
        assertNull(result.getField(4), "errorStackTrace should be null");

        String jsonStr = (String) result.getField(0);
        assertTrue(jsonStr.contains("\"hostname\" : \"localhost\""));
        assertTrue(jsonStr.contains("\"type\" : \"USER_END\""));
        assertTrue(jsonStr.contains("\"uniqueId\" : 20894"));
        assertTrue(jsonStr.contains("\"attributes\""));


/*
        List<String> list = new ArrayList<>();
        list.add("""
                type=SERVICE_STOP msg=audit(1754467298.844:313084): pid=1 uid=0 auid=4294967295 ses=4294967295 subj=unconfined msg='unit=fwupd comm="systemd" exe="/usr/lib/systemd/systemd" hostname=? addr=? terminal=? res=success'UID="root" AUID="unset"
                """);
        list.add(""" 
                type=AVC msg=audit(1754467299.008:313085): apparmor="ALLOWED" operation="open" profile="/usr/sbin/sssd" name="/proc/3779461/cmdline" pid=1368 comm="sssd_nss" requested_mask="r" denied_mask="r" fsuid=0 ouid=0FSUID="root" OUID="root" 
                """);
        list.add(""" 
                type=AVC msg=audit(1754467671.021:9728): apparmor="ALLOWED" operation="open" profile="/usr/sbin/sssd" name="/proc/5359/cmdline" pid=1261 comm="sssd_nss" requested_mask="r" denied_mask="r" fsuid=0 ouid=0FSUID="root" OUID="root"
                """);
        list.add("""
                type=AVC msg=audit(1754467671.617:9729): apparmor="ALLOWED" operation="open" profile="/usr/sbin/sssd" name="/proc/3845/cmdline" pid=1261 comm="sssd_nss" requested_mask="r" denied_mask="r" fsuid=0 ouid=0FSUID="root" OUID="root"
                """);
        list.add("""
                type=AVCX msg=audit(1754467713.029:313086): apparmor="ALLOWED" operation="exec" profile="/usr/sbin/sssd" name="/usr/bin/nsupdate" pid=1773134 comm="sssd_be" requested_mask="x" denied_mask="x" fsuid=0 ouid=0 target="/usr/sbin/sssd//null-/usr/bin/nsupdate"FSUID="root" OUID="root"
                """);
        list.stream().forEach(entry -> System.out.println(udf.eval("localhost", entry))); */
    }

    @Test
    void testUnexpectedCharacterInAttributes() {
        String line =  "type=USER_END msg=audit(1754395786.670:20894): pid=11333 uid=0 auid=4294967295 ses=4294967295 subj=unconfined msg='op=PAM:session_close grantors=pam_permit,pam_umask,pam_winbind acct=\"testuser1\" exe=\"/usr/sbin/smbd\" hostname=10.144.86.74 addr=10.144.86.74 terminal=smb/3802623126 res=success'\u001dUID$=\"root\" AUID=\"unset\"";

        Row result = udf.eval("host1", line);

        assertNull(result.getField(0));
        assertEquals("PARSING_ERROR", result.getField(1));
        assertEquals("java.lang.IllegalArgumentException", result.getField(2));
        assertTrue(((String) result.getField(3)).contains("$"), "Input has unexpected character '$'");
        assertNotNull(result.getField(4), "Stack trace captured");
    }

    @Test
    void testEmptyLine() {
        Row result = udf.eval("host1", "   ");

        assertNull(result.getField(0));
        assertEquals("EMPTY_INPUT", result.getField(1));
        assertNull(result.getField(2));
        assertEquals("Input line is null or blank", result.getField(3));
        assertNull(result.getField(4));
    }

}
