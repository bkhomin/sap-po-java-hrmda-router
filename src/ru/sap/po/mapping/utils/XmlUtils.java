package ru.sap.po.mapping.utils;

import org.w3c.dom.Document;

import java.io.InputStream;
import java.io.StringWriter;

import java.util.Optional;

import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import com.sap.aii.mapping.api.TransformationInput;
import com.sap.aii.mapping.api.AbstractTrace;

public final class XmlUtils {
	
	// Empty private constructor to prevent class instance creation
	private XmlUtils() {}
	
	/**
	 * Method that parses DOM <tt>Document</tt> to <tt>String</tt>
	 * wrapped by <tt>Optional</tt>
	 * 
	 * @param doc - Document object instance
	 * @param trace - AbstractTrace object instance
	 * 
	 * @return Optional<String>
	 */
	public static Optional<String> getXmlStringFromDocument(Document doc, 
			AbstractTrace trace) {
	    try {
	       DOMSource domSource = new DOMSource(doc);
	       StringWriter writer = new StringWriter();
	       StreamResult result = new StreamResult(writer);
	       TransformerFactory tf = TransformerFactory.newInstance();
	       Transformer transformer = tf.newTransformer();
	       transformer.transform(domSource, result);
	       return Optional.of(writer.toString());
	    } catch(TransformerException ex) {
	       trace.addWarning("Encountered error while trying to parse DOM Document to XML string ", ex);
	       return Optional.empty();
	    }
    }
	
	/**
	 * Method that parses incoming message from <tt>TransformationInput</tt>
	 * to DOM <tt>Document</tt> wrapped by <tt>Optional</tt>
	 * 
	 * @param ti - TransformationInput object instance
	 * @param trace - AbstractTrace object instance
	 * 
	 * @return Optional<Document>
	 */
	public static Optional<Document> getDocumentFromTransformationInput(TransformationInput ti, 
			AbstractTrace trace) {		
		trace.addDebugMessage("Started to parse HRMD_A09 XML to DOM Document.");		
		try (InputStream is = (InputStream) ti.getInputPayload().getInputStream()){			
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(is);			
			trace.addDebugMessage("Finished parsing of HRMD_A09 XML to DOM Document.");			
			return Optional.of(doc);
		} catch (Exception e) {
			trace.addWarning("Encountered error during incoming message parsing ", e);
			return Optional.empty();
		}
	}
	
}