package ru.sap.po.mapping;

import ru.sap.po.mapping.utils.*;
import ru.sap.po.mapping.config.*;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import com.sap.aii.mapping.api.*;
import com.sap.aii.mapping.lookup.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HRMD_to_ReceiverDetermination extends AbstractTransformation {
	
	/** 
	 * Variable that keep Communication Component parameter name of SAP PO itself in Integration Directory
	 * 
	 * Change its value after transfer SAP PO configuration objects through your system landscape
	 * to appropriate Communication Component name in "mapping.properties" file.
	 */
	private String PO_COMMUNICATION_COMPONENT;
	
	/** 
	 * Variable that keep Communication Channel parameter name of configured SOAP CC that
	 * contacts internal <tt>IntegratedConfiguration750In</tt> SOAP interface.
	 * 
	 * 
	 * Change its value after transfer SAP PO configuration objects through your system landscape
	 * to appropriate Communication Channel name in "mapping.properties" file.
	 */
	private String PO_ICO_COMMUNICATION_CHANNEL;
	
	/** 
	 * List of <tt>HRMD_A09</tt> organizational management infotypes.
	 * 
	 * Can be modified in "mapping.properties" file
	 */
	private List<String> MANAGEMENT_INFOTYPES;
	
	// Mapping properties handler object
	private PropertiesHandler propHandler;
	
	// SAP PO logging object
	private AbstractTrace trace;
	
	// Array with receiver business systems
	private List<String> receivers = null;
	
	@Override
	public void transform (TransformationInput ti, TransformationOutput to) 
			throws StreamTransformationException {
		
		// Get trace object instance
		trace = (AbstractTrace) getTrace();		
		trace.addInfo("HRMD_A to ReceiverDetermination mapping program started!");
		
		// Try to load mapping properties from file - if it fails, we'll stop the whole transformation
		if(!loadProperties(trace)) return;

		// Get <tt>InputParameters</tt> from <tt>TransformationInput</tt>
		InputParameters ip = ti.getInputParameters();
		
		// Parse incoming message to DOM <code>Document</code>
		Optional<Document> hrDocOp = XmlUtils.getDocumentFromTransformationInput(ti, trace);
		
		// If parsing failed - there's nothing to process, so we'll stop the whole transformation
		if (!hrDocOp.isPresent()) return;
		
		// If there's at least one management segment - message will be routed to ALL possible receivers
		// Else - try to determine receivers by company code
		if (!checkManagementInfotypes(hrDocOp.get(), trace))
			receivers = getReceiversFromDocument(hrDocOp.get(), ip, trace);				
				
		// Check if we've got company codes and if not - get all possible receiver systems
		if (receivers == null || receivers.isEmpty()) {
			trace.addDebugMessage("Could not determine receiver systems by management infotypes or company code, "
					+ "so try to perform lookup for all possible receiver systems, configured in ICo.");			
			try {
				receivers = lookupAllPossibleReceiversOfScenario(ti, trace);
			} catch (LookupException le) {
				trace.addWarning("Encountered error while tried to lookup for all possible receivers " + le);
			}
		}
		
		// Write xml to output stream
		writeXMLToTransformationOutput(to, generateReceiversXML(receivers), trace);
		
		trace.addInfo("HRMD_A to ReceiverDetermination mapping program finished!");
	}
	
	/**
	 * Method loads mapping parameters from "mapping.properties" file.
	 * Will return <code>false</code>, if any error appear.
	 *  
	 * @param trace - AbstractTrace object instance
	 * 
	 * @return boolean
	 */
	private boolean loadProperties(AbstractTrace trace) {
		
		propHandler = PropertiesHandler.getInstance();
		
		Optional<String> bsProp = propHandler.getValue("lookup.sappo.component.name");
		if (!bsProp.isPresent()) {
			trace.addWarning("Can't load Communication Component property from \'mapping.properties\' file.");
			return false;
		}
		
		Optional<String> ccProp = propHandler.getValue("lookup.sappo.channel.ico.name");
		if (!ccProp.isPresent()) {
			trace.addWarning("Can't load Communication Channel property from \'mapping.properties\' file.");
			return false;
		}
		
		Optional<String> mgmtInftyProp = propHandler.getValue("management.infotypes");
		if (!mgmtInftyProp.isPresent()) {
			trace.addWarning("Can't load Management Infotypes property from \'mapping.properties\' file.");
			return false;
		}
		
		PO_COMMUNICATION_COMPONENT = bsProp.get();
		trace.addDebugMessage("Loaded Communication Component property with value: \' " + PO_COMMUNICATION_COMPONENT + "\' successfully");
		
		PO_ICO_COMMUNICATION_CHANNEL = ccProp.get();
		trace.addDebugMessage("Loaded Communication Channel property with value: \' " + PO_ICO_COMMUNICATION_CHANNEL + "\' successfully");
		
		String[] mgmtInfty = propHandler.readPropertyValueToArray(mgmtInftyProp.get());
		MANAGEMENT_INFOTYPES = Arrays.asList(mgmtInfty);
		trace.addDebugMessage("Loaded Management Infotypes property with value: \' " + Arrays.toString(mgmtInfty) + "\' successfully");		
		
		return true;
	}
	
	/**
	 * Method checks if incoming message has organizational management infotypes.
	 * 
	 * If AT LEAST ONE of that records is found - return <code>true</code>.
	 * 
	 * This mean that we need to route incoming message to ALL possible
	 * receivers.
	 * 
	 * @param doc - Document object instance
	 * @param trace - AbstractTrace object instance
	 * 
	 * @return boolean
	 */
	private boolean checkManagementInfotypes(Document doc, AbstractTrace trace) {
		
		// Get all <tt>E1PITYP</tt> segments from the whole DOM tree
		NodeList infoTypes = doc.getElementsByTagName("E1PITYP");
		
		trace.addDebugMessage("Incoming IDOC message has " + infoTypes.getLength() + " segments.");
		
		// Iterate through each <tt>E1PITYP</tt> node
		for (int i = 0; i < infoTypes.getLength(); i++) {
			Node infoType = infoTypes.item(i);
			
			if (infoType.getNodeType() == Node.ELEMENT_NODE) {
				Element infoTypeEl = (Element) infoType;
				
				// Try to get <tt>INFTY</tt> string for segment
				Optional<String> infoTypeCode = Optional.of(infoTypeEl.getElementsByTagName("INFTY").item(0).getTextContent());
				
				// Go to next iteration, if there's no <tt>INFTY</tt> string
				if (!infoTypeCode.isPresent() || "".equals(infoTypeCode.get())) continue;
		
				// Perform check
				if (MANAGEMENT_INFOTYPES.contains(infoTypeCode.get())) {					
					trace.addInfo("Found one of " + Arrays.toString(MANAGEMENT_INFOTYPES.toArray()) + " segment, "
							+ "so IDOC must be routed to all possible receivers, configured in ICo.");
					return true;
				}
			}
		}
		
		trace.addInfo("Could not find any of " + Arrays.toString(MANAGEMENT_INFOTYPES.toArray()) + " segment, "
				+ "so receiver determination algorythm must be continued.");		
		return false;
	}
	
	/**
	 * Method walks through DOM <tt>Document</tt> that was parsed from incoming IDOC message, 
	 * iterates over each <tt>E1PITYP</tt> segment, matches all segments with 
	 * <tt>INFTY</tt> == '0001', collects all values of element <tt>BUKRS</tt> and
	 * try to get appropriate receiver system value from Operation Mapping parameters.
	 * Collects all found receiver systems into <tt>List</tt> of <tt>String</tt> values.
	 * 
	 * @param doc - Document object instance
	 * @param ip - InputParameters object instance
	 * @param trace - AbstractTrace object instance
	 * 
	 * @return List<String> - list of all company codes 
	 * to which message should be routed
	 */
	private List<String> getReceiversFromDocument(Document doc, InputParameters ip, AbstractTrace trace) {
		
		trace.addDebugMessage("Started to parse HRMD_A09 XML and collecting all receiver company codes.");
		
		// Declare and instantiate company codes list
		List<String> receivers = new ArrayList<>();
				
		// Get all <tt>E1PITYP</tt> segments from the whole DOM tree
		NodeList infoTypes = doc.getElementsByTagName("E1PITYP");
		
		trace.addDebugMessage("Incoming IDOC message has " + infoTypes.getLength() + " segments.");
		
		// Iterate through each <tt>E1PITYP</tt> node
		for (int i = 0; i < infoTypes.getLength(); i++) {
			Node infoType = infoTypes.item(i);
			
			if (infoType.getNodeType() == Node.ELEMENT_NODE) {
				Element infoTypeEl = (Element) infoType;
				
				// Try to get <tt>INFTY</tt> string for segment
				Optional<String> infoTypeCode = Optional.of(infoTypeEl.getElementsByTagName("INFTY").item(0).getTextContent());
				
				// Go to next iteration, if there's no <tt>INFTY</tt> string
				if (!infoTypeCode.isPresent() || "".equals(infoTypeCode.get())) continue;

				// If '0001' infotype is found, get company code and then get appropriate SystemID from <tt>InputParameters</tt>
				if ("0001".equals(infoTypeCode.get())) {						
					Optional<String> companyCode = Optional.of(infoTypeEl.getElementsByTagName("BUKRS").item(0).getTextContent());							
					if (companyCode.isPresent()) {
						try {
							Optional<String> systemId = Optional.of(ip.getString("R" + companyCode.get()));
							if (systemId.isPresent() && !receivers.contains(systemId.get()))
								receivers.add(systemId.get());
						} catch (UndefinedParameterException upe) {
							trace.addWarning("Encountered error while tried to get input parameter " + upe);			
						}							
					}
				}
			}
		}
		
		trace.addDebugMessage("Finished parsing of HRMD_A09 XML. Collected " + receivers.size() + " receiver(s).");
		
		return receivers;
	}
	
	/**
	 * Method that performs lookup for all possible receiver systems 
	 * in current ICo object by accessing pre-configured Communication 
	 * Channel in ID with generated XML payload.
	 * 
	 * 
	 * @param ti - TransformationInput object instance
	 * @param trace - AbstractTrace object instance
	 * 
	 * @return List<String> - list of all possible receiver systems, configured in ICo
	 * @throws LookupException
	 */
	private List<String> lookupAllPossibleReceiversOfScenario(TransformationInput ti, AbstractTrace trace) throws LookupException {
		
		trace.addDebugMessage("Started to lookup for all possible receiver systems, configured in ICo.");
		
		// Declare and instantiate receiver systems list
		List<String> allPossibleReceivers = new ArrayList<>();
		
		// Get InputHeader from TransformationInput
		InputHeader ih = ti.getInputHeader();
		
		// Instantiate StringBuilder instance to build lookup request XML
		StringBuilder sb = new StringBuilder();
		
		sb.append("<bas:IntegratedConfigurationReadRequest xmlns:bas=\"http://sap.com/xi/BASIS\">");
		sb.append("<IntegratedConfigurationID>");
		sb.append("<SenderComponentID>" + ih.getSenderService() + "</SenderComponentID>");
		sb.append("<InterfaceName>" + ih.getInterface() + "</InterfaceName>");
		sb.append("<InterfaceNamespace>" + ih.getInterfaceNamespace() + "</InterfaceNamespace>");
		sb.append("</IntegratedConfigurationID>");
		sb.append("</bas:IntegratedConfigurationReadRequest>");
		
		// Declare XmlPayload objects
		XmlPayload xmlRequest = null;
		XmlPayload xmlResponse = null;
		
		// Convert StringBuilder to XmlPayload
		try (InputStream is = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"))) {
			xmlRequest = LookupService.getXmlPayload(is);
		} catch (IOException ioe) {
			trace.addWarning("Encountered error during convertiong String to XmlPayload while lookup ", ioe);
		}
		
		Optional<Channel> lookupChannel = Optional.of(LookupService.getChannel(PO_COMMUNICATION_COMPONENT, PO_ICO_COMMUNICATION_CHANNEL));
		if (lookupChannel.isPresent()) {
			SystemAccessor sa = LookupService.getSystemAccessor(lookupChannel.get());
			if (sa != null && xmlRequest != null) {
				xmlResponse = (XmlPayload) sa.call(xmlRequest);
				sa.close();
			}
		}
		
		try (InputStream is = xmlResponse.getContent()) {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(is);
			
			if (doc.hasChildNodes()) {
				NodeList receiverInterfaces = doc.getElementsByTagName("ReceiverInterfaces");
				
				trace.addDebugMessage("ICo lookup returned " + receiverInterfaces.getLength() + " receiver interfaces.");
				
				for (int i = 0; i < receiverInterfaces.getLength(); i++) {
					Node receiverInterface = receiverInterfaces.item(i);
					
					if (receiverInterface.getNodeType() == Node.ELEMENT_NODE) {
						Element receiverInterfaceEl = (Element) receiverInterface;						
						Optional<String> systemId = Optional.of(receiverInterfaceEl.getElementsByTagName("ComponentID").item(0).getTextContent());						
						if (systemId.isPresent() && !allPossibleReceivers.contains(systemId.get()))
							allPossibleReceivers.add(systemId.get());
					}
				}
			}
			
		} catch (Exception e) {
			trace.addWarning("Encountered error during reading lookup content ", e);
		}
		
		trace.addDebugMessage("Finished lookup for all possible receiver systems. Collected " + allPossibleReceivers.size() + " receiver systems.");
		
		return allPossibleReceivers;		
	}

	/**
	 * Method that writes XML message of type <tt>ReceiverDetermination</tt> 
	 * after mapping into <tt>TransformationOutput</tt> object instance.
	 * 
	 * @param to - TransformationOutput object instance
	 * @param xml - String target XML message payload
	 * @param trace - AbstractTrace object instance
	 * 
	 * @return void
	 */
	private void writeXMLToTransformationOutput(TransformationOutput to, String xml, AbstractTrace trace) {
		try (OutputStream os = (OutputStream) to.getOutputPayload().getOutputStream()) {			
			os.write(xml.getBytes("UTF-8"));
			trace.addDebugMessage("Finished writing result message to \'TransformationOutput\'");
		} catch (Exception e) {
			trace.addWarning("Encountered error during writing to TransformationOutput ", e);
		}
	}
	
	/**
	 * Method that generates XML <tt>String</tt> of type <tt>ReceiverDetermination</tt>.
	 * 
	 * @param receivers - List<String> list of all receiver systems
	 * 
	 * @return String - list of all Business Systems 
	 * to which message should be routed
	 */
	private String generateReceiversXML(List<String> receivers) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		sb.append("<ns1:Receivers xmlns:ns1=\"http://sap.com/xi/XI/System\">");
		
		for (String receiver: receivers) {
			if (receiver == "" || receiver == null) continue;
			
			sb.append("<Receiver><Service>");
			sb.append(receiver);
			sb.append("</Service></Receiver>");
		}
		
		sb.append("</ns1:Receivers>");
		
		return sb.toString();
	}
	
}
