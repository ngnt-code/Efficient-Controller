
/*
 * Copyright (c) 2013-2014 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.controller.sal.packet;

import java.util.Arrays;

import junit.framework.Assert;

import org.junit.Test;

public class ICMPTest {

    @Test
    public void testSetTypeCode() {
        ICMP icmp = new ICMP();
        byte icmpType = 2;
        icmp.setType(icmpType);
        byte[] typeCode = icmp.hdrFieldsMap.get("Type");
        Assert.assertTrue(typeCode[0] == 2);

    }

    @Test
    public void testSetChecksum() {
        ICMP icmp = new ICMP();
        short icmpChecksum = 200;
        icmp.setChecksum(icmpChecksum);
        byte[] checksum = icmp.hdrFieldsMap.get("Checksum");
        Assert.assertTrue(checksum[0] == 0);
        Assert.assertTrue(checksum[1] == -56);

    }

    @Test
    public void testSetIdentifier() {
        ICMP icmp = new ICMP();
        short icmpIdentifier = 1201;
        icmp.setIdentifier(icmpIdentifier);
        byte[] identifier = icmp.hdrFieldsMap.get("Identifier");
        Assert.assertTrue(identifier[0] == 4);
        Assert.assertTrue(identifier[1] == -79);

    }

    @Test
    public void testSetSequenceNumber() {
        ICMP icmp = new ICMP();
        short icmpSequenceNumber = 5000;
        icmp.setSequenceNumber(icmpSequenceNumber);
        byte[] sequenceNumber = icmp.hdrFieldsMap.get("SequenceNumber");
        Assert.assertTrue(sequenceNumber[0] == 19);
        Assert.assertTrue(sequenceNumber[1] == -120);

    }

    @Test
    public void testSerialization() throws PacketException {
        byte icmpRawPayload[] = { (byte) 0x38, (byte) 0x26, (byte) 0x9e,
                (byte) 0x51, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x2e, (byte) 0x6a, (byte) 0x08,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x10, (byte) 0x11, (byte) 0x12,
                (byte) 0x13, (byte) 0x14, (byte) 0x15, (byte) 0x16,
                (byte) 0x17, (byte) 0x18, (byte) 0x19, (byte) 0x1a,
                (byte) 0x1b, (byte) 0x1c, (byte) 0x1d, (byte) 0x1e,
                (byte) 0x1f, (byte) 0x20, (byte) 0x21, (byte) 0x22,
                (byte) 0x23, (byte) 0x24, (byte) 0x25, (byte) 0x26,
                (byte) 0x27, (byte) 0x28, (byte) 0x29, (byte) 0x2a,
                (byte) 0x2b, (byte) 0x2c, (byte) 0x2d, (byte) 0x2e,
                (byte) 0x2f, (byte) 0x30, (byte) 0x31, (byte) 0x32,
                (byte) 0x33, (byte) 0x34, (byte) 0x35, (byte) 0x36, (byte) 0x37 };
        serializeTest(icmpRawPayload, (short)0xe553);

        serializeTest(null, (short)0xb108);
        serializeTest(new byte[0], (short)0xb108);

        byte[] odd = {
            (byte)0xba, (byte)0xd4, (byte)0xc7, (byte)0x53,
            (byte)0xf8, (byte)0x59, (byte)0x68, (byte)0x77,
            (byte)0xfd, (byte)0x27, (byte)0xe0, (byte)0x5b,
            (byte)0xd0, (byte)0x2e, (byte)0x28, (byte)0x41,
            (byte)0xa3, (byte)0x48, (byte)0x5d, (byte)0x2e,
            (byte)0x7d, (byte)0x5b, (byte)0xd3, (byte)0x60,
            (byte)0xb3, (byte)0x88, (byte)0x8d, (byte)0x0f,
            (byte)0x1d, (byte)0x87, (byte)0x51, (byte)0x0f,
            (byte)0x6a, (byte)0xff, (byte)0xf7, (byte)0xd4,
            (byte)0x40, (byte)0x35, (byte)0x4e, (byte)0x01,
            (byte)0x36,
        };
        serializeTest(odd, (short)0xd0ad);

        // Large payload that causes 16-bit checksum overflow more than
        // 255 times.
        byte[] largeEven = new byte[1024];
        Arrays.fill(largeEven, (byte)0xff);
        serializeTest(largeEven, (short)0xb108);

        byte[] largeOdd = new byte[1021];
        Arrays.fill(largeOdd, (byte)0xff);
        serializeTest(largeOdd, (short)0xb207);
    }

    private void serializeTest(byte[] payload, short checksum)
        throws PacketException {
        ICMP icmp = new ICMP();
        icmp.setType((byte)8).setCode((byte)0).
            setIdentifier((short)0x46f5).setSequenceNumber((short)2);
        int payloadSize = 0;
        if (payload != null) {
            icmp.setRawPayload(payload);
            payloadSize = payload.length;
        }

        // Serialize
        byte[] data = icmp.serialize();
        Assert.assertEquals(payloadSize + 8, data.length);

        // Deserialize
        ICMP icmpDes = new ICMP();
        icmpDes.deserialize(data, 0, data.length);

        Assert.assertFalse(icmpDes.isCorrupted());
        Assert.assertEquals(checksum, icmpDes.getChecksum());
        Assert.assertEquals(icmp, icmpDes);
    }
}
