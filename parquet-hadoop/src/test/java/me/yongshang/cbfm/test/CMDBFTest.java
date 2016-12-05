/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package me.yongshang.cbfm.test;

import me.yongshang.cbfm.CMDBF;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by yongshangwu on 2016/11/23.
 */
public class CMDBFTest {
    @Test
    public void testFunctionality() throws IOException {
        int elementCount = 100;
        CMDBF.ON = true;
        CMDBF.desiredFalsePositiveProbability = 0.1;
        CMDBF.dimensions = new String[]{"A", "B", "C"};
        CMDBF index = new CMDBF(elementCount);

        index.insert(new String[]{"A", "B"}, new byte[][]{
                {1},
                {2}
        });

        assertFalse(index.contains(CMDBF.dimensions, new byte[][]{
                {2},
                {2},
                {3}
        }));
        assertTrue(index.contains(new String[]{"A", "B"}, new byte[][]{
                {1},
                {2}
        }));
        index.serialize(new DataOutputStream(new FileOutputStream(new File("/Users/yongshangwu/Desktop/cmdbf"))));

        index = new CMDBF(new DataInputStream(new FileInputStream(new File("/Users/yongshangwu/Desktop/cmdbf"))));
        assertTrue((index.contains(new String[]{"A", "B"}, new byte[][]{
                {1},
                {2}
        })));
    }

    @Test
    public void testMassively(){

    }
}
