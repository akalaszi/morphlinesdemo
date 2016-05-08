/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package morphlinesdemo;

import java.io.*;
import java.net.URISyntaxException;

import com.cloudera.cdk.morphline.api.*;
import com.cloudera.cdk.morphline.base.*;
import com.cloudera.cdk.morphline.base.Compiler;

public class MorphlinesDemo {

	public static void main(String[] args) throws IOException {

//		File morphlineFile = fileFrom("readLine.conf");
		File morphlineFile = fileFrom("readMultiLine.conf");
		
		File inputFile = fileFrom("input.txt");

		String morphlineId = null;
		MorphlineContext morphlineContext = new MorphlineContext.Builder().build();
		Command morphline = new Compiler().compile(morphlineFile, morphlineId, morphlineContext, null);

		// process each input data file
		Notifications.notifyBeginTransaction(morphline);
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(inputFile));
			Record record = new Record();
			record.put(Fields.ATTACHMENT_BODY, in);
			Notifications.notifyStartSession(morphline);
			boolean success = morphline.process(record);
			if (!success) {
				System.out.println("Morphline failed to process record: " + record);
			}
			in.close();
			Notifications.notifyCommitTransaction(morphline);
		} catch (RuntimeException e) {
			Notifications.notifyRollbackTransaction(morphline);
			morphlineContext.getExceptionHandler().handleException(e, null);
		}
		Notifications.notifyShutdown(morphline);
	}

	private static File fileFrom(String name) throws IOException  {
		try {
			return new File(MorphlinesDemo.class.getResource(name).toURI().getPath());
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}
}