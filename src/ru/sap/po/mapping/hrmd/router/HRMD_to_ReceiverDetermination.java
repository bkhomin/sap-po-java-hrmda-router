package ru.sap.po.mapping.hrmd.router;

import org.xml.sax.SAXException;
import ru.sap.po.mapping.hrmd.router.config.RouterPropertiesHandler;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.sap.aii.mapping.api.*;
import com.sap.aii.mapping.lookup.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class HRMD_to_ReceiverDetermination extends AbstractTransformation {

	/**
	 * Constant represents the Last Day on Earth according to SAP.
	 * Is used to determine the most actual time-dependent infosegments.
	 */
	private final String lastSapDayOnEarth = "99991231";

	/**
	 * Constant represents the namespace of Dynamic Configuration key.
	 * DC is used to store 'BUKRS'-'ReceiverSystem' pairs.
	 */
	private final String dcKeyNamespace = "urn:ru:SAP:CustomNamespace:10";

	/**
	 * Variable that keep Communication Component parameter name of SAP PO itself in Integration Directory
	 *
	 * Change its value after transfer SAP PO configuration objects through your system landscape
	 * to appropriate Communication Component name in "router.properties" file.
	 */
	private String LOOKUP_SERVICE;

	/**
	 * Variable that keep Communication Channel parameter name of configured SOAP CC that
	 * contacts internal <tt>IntegratedConfiguration750In</tt> SOAP interface.
	 *
	 *
	 * Change its value after transfer SAP PO configuration objects through your system landscape
	 * to appropriate Communication Channel name in "router.properties" file.
	 */
	private String LOOKUP_CHANNEL;

	/**
	 * List of <tt>HRMD_A09</tt> organizational management infotypes.
	 *
	 * Can be modified in "router.properties" file
	 */
	private List<String> MANAGEMENT_INFOTYPES;

	/**
	 * Map of BUKRS and SystemID correspondences.
	 */
	private Map<String, String> receivers = null;

	/**
	 * A variable indicates that message should be routed to all
	 * possible receiver systems or not.
	 */
	private boolean routeToAll = false;

	@Override
	public void transform (TransformationInput ti, TransformationOutput to)
			throws StreamTransformationException {

		getTrace().addInfo("HRMD_A to ReceiverDetermination mapping program started!");

		// Try to load mapping properties from file - if it fails, we'll stop the whole transformation
		if(!loadProperties()) return;

		// Parse incoming message to DOM <code>Document</code>
		Document source = getDocumentFromTransformationInput(ti);

		// If parsing failed - there's nothing to process, so we'll stop the whole transformation
		if (source == null) return;

		// Try to collect receivers Map from incoming IDOC message and InputParameters
		getReceiversFromDocument(source, ti.getInputParameters());

		// Check if we've got company codes and if yes - put them to Dynamic Configuration
		if (!receivers.isEmpty()) addReceiversToDynamicConfiguration(ti.getDynamicConfiguration());

		// If at least one management infotype is found - lookup for all possible receivers
		if (routeToAll) lookupAllPossibleReceiversOfScenario(ti);

		// Check again if we have any receivers. If not - we'll stop the whole transformation
		if (!receivers.isEmpty()) {
			// Write xml to output stream
			writeXMLToTransformationOutput(to, generateReceiversXML());
			getTrace().addInfo("HRMD_A to ReceiverDetermination mapping program finished!");
		} else {
			getTrace().addWarning("Could not determine any receiver system!");
		}
	}

	/**
	 * Method walks through DOM {@link Document} that was parsed from incoming IDOC message,
	 * collects all <code>E1PLOGI</code> elements, performs segmentation of those by
	 * "OTYPE" characteristic. Then method collects <code>E1PITYP</code> segments
	 * for persons and orgmanagement separately and passes them to one of special
	 * processing methods: {@link #processPersonInfoTypes(List, InputParameters)} for persons
	 * and {@link #processOrgManagementInfoTypes(List)} for orgmanagement.
	 *
	 * @param source   {@link Document} object instance
	 * @param ip       {@link InputParameters} object instance
	 */
	private void getReceiversFromDocument(Document source, InputParameters ip) {

		getTrace().addDebugMessage("Started to parse HRMD_A09 XML and collecting all receiver company codes.");

		// Instantiate company codes Map
		receivers = new HashMap<>();

		// Get all <tt>E1PLOGI</tt> segments from the whole DOM tree
		NodeList segmentNodes = source.getElementsByTagName("E1PLOGI");

		// Collect all <tt>E1PLOGI</tt> nodes into HashMap<Boolean, List<Node>> with 2 entries:
		// true: List<Node> - entry with List of all E1PLOGI nodes that have OTYPE=="P", e.g. all person segments
		// false: List<Node> - entry with List of all E1PLOGI nodes that have any other OTYPE
		Map<Boolean, List<Node>> segmentsMap = IntStream.range(0, segmentNodes.getLength())
				.mapToObj(segmentNodes::item)
				.collect(Collectors.toList())
				.stream()
				.collect(Collectors.partitioningBy(node -> {
					if (node.getNodeType() != Node.ELEMENT_NODE) return false;
					Element segment = (Element) node;
					String oType = Optional.ofNullable(segment.getElementsByTagName("OTYPE"))
							.map(chldrn -> chldrn.item(0))
							.map(Node::getTextContent)
							.orElse("");
					return "P".equals(oType);
				}));

		// Iterate over collected Map to collect (again) all <tt>E1PITYP</tt> Elements and process them separately
		segmentsMap.forEach((isPersons, segments) -> {
			if (segments.isEmpty()) return;

			List<Element> infoTypesToProcess = new ArrayList<>();

			// Collect all <tt>E1PITYP</tt> Elements from each <tt>E1PLOGI</tt> node
			segments.forEach(E1PLOGI -> {
				if (E1PLOGI.getNodeType() != Node.ELEMENT_NODE) return;
				Element segment = (Element) E1PLOGI;
				NodeList infoTypes = segment.getElementsByTagName("E1PITYP");
				IntStream.range(0, infoTypes.getLength())
						.mapToObj(infoTypes::item)
						.collect(Collectors.toList())
						.forEach(E1PITYP -> {
							if (E1PITYP.getNodeType() != Node.ELEMENT_NODE) return;
							Element infoType = (Element) E1PITYP;
							infoTypesToProcess.add(infoType);
						});
			});

			if (isPersons) {
				processPersonInfoTypes(infoTypesToProcess, ip);
			} else {
				processOrgManagementInfoTypes(infoTypesToProcess);
			}
		});

		getTrace().addDebugMessage("Finished parsing of HRMD_A09 XML. Collected " + receivers.size() + " receiver(s).");
	}

	/**
	 * Method iterates over each <code>E1PITYP</code> segment collected previously (with "OTYPE" == "P"),
	 * matches all segments with <code>INFTY</code> == '0001', collects all values of element <code>BUKRS</code> and
	 * try to get appropriate receiver system value from Operation Mapping parameters.
	 * Collects all found receiver systems into {@link Map} of {@link String} - {@link String} values. <br><br>
	 *
	 * @param infoTypes {@link List} of collected previously infotypes
	 *                           that have "OTYPE" == "P"
	 * @param ip {@link InputParameters} routing table from ICo and OM configuration
	 */
	private void processPersonInfoTypes(List<Element> infoTypes, InputParameters ip) {
		infoTypes.forEach(infoType -> {
			// Try to get <tt>INFTY</tt> string for segment
			String infoTypeCode = getTextContentFromElementTag(infoType, "INFTY");

			// Go to next iteration, if there's no <tt>INFTY</tt> string
			if (isNullOrEmpty(infoTypeCode)) return;

			// If '0001' infotype is found, get company code and then get appropriate SystemID from <tt>InputParameters</tt>
			if ("0001".equals(infoTypeCode)) {
				NodeList timeDependentSegments = infoType.getElementsByTagName("E1P0001");

				List<Node> timeDependentSegmentsList = IntStream.range(0, timeDependentSegments.getLength())
						.mapToObj(timeDependentSegments::item)
						.collect(Collectors.toList());

				timeDependentSegmentsList.forEach(E1P0001 -> {
					if (E1P0001.getNodeType() != Node.ELEMENT_NODE) return;
					Element timeDependentSegment = (Element) E1P0001;

					String endDate = getTextContentFromElementTag(timeDependentSegment, "ENDDA");
					if(!lastSapDayOnEarth.equals(endDate)) return;

					String companyCode = getTextContentFromElementTag(timeDependentSegment, "BUKRS");
					if (!isNullOrEmpty(companyCode)) {
						try {
							String systemId = ip.getString("R" + companyCode);
							if (systemId != null && !receivers.containsKey(companyCode)) {
								receivers.put(companyCode, systemId);
								getTrace().addDebugMessage("Added BUKRS: '" + companyCode +
										"' and SystemID: '" + systemId + "' to result receivers map.");
							}
						} catch (UndefinedParameterException upe) {
							getTrace().addWarning("Encountered UndefinedParameterException while tried to get input parameter " + upe);
						}
					}
				});
			}
		});
	}

	/**
	 * Method checks if incoming message has organizational management infotypes.
	 * If AT LEAST ONE of that records is found - set {@code true} to {@link #routeToAll}
	 * variable. This mean that we need to route incoming message to ALL possible
	 * receivers.
	 *
	 * @param infoTypes {@link List} of collected previously infotypes
	 *                              that have "OTYPE" != "P"
	 */
	private void processOrgManagementInfoTypes(List<Element> infoTypes) {
		infoTypes.forEach(infoType -> {
			// Try to get <tt>INFTY</tt> string for segment
			String infoTypeCode = getTextContentFromElementTag(infoType, "INFTY");

			// Perform check fo management infotypes
			if (!routeToAll && MANAGEMENT_INFOTYPES.contains(infoTypeCode)) {
				getTrace().addInfo("Found one of " + Arrays.toString(MANAGEMENT_INFOTYPES.toArray()) + " segment, "
						+ "so IDOC must be routed to all possible receivers, configured in ICo.");
				routeToAll = true;
			}
		});
	}

	/**
	 * Method that performs SOAP lookup for all possible receiver systems
	 * in current ICo object by accessing pre-configured Communication
	 * Channel in Integration Directory with generated XML payload.
	 *
	 * @param ti {@link TransformationInput} object instance
	 * @throws LookupException if SOAP lookup fails at any point
	 */
	private void lookupAllPossibleReceiversOfScenario(TransformationInput ti) throws LookupException {

		getTrace().addDebugMessage("Started to lookup for all possible receiver systems, configured in ICo.");

		// Get InputHeader from TransformationInput
		InputHeader ih = ti.getInputHeader();

		// Declare XmlPayload objects
		XmlPayload xmlRequest = null;
		XmlPayload xmlResponse = null;

		// Construct XML request body String
		String xmlRequestBody = "<bas:IntegratedConfigurationReadRequest xmlns:bas=\"http://sap.com/xi/BASIS\">" +
				"<IntegratedConfigurationID>" +
				"<SenderComponentID>" + ih.getSenderService() + "</SenderComponentID>" +
				"<InterfaceName>" + ih.getInterface() + "</InterfaceName>" +
				"<InterfaceNamespace>" + ih.getInterfaceNamespace() + "</InterfaceNamespace>" +
				"</IntegratedConfigurationID>" +
				"</bas:IntegratedConfigurationReadRequest>";

		// Open ByteArrayInputStream from constructed request payload string
		try (InputStream is = new ByteArrayInputStream(xmlRequestBody.getBytes(StandardCharsets.UTF_8))) {
			// Convert ByteArrayInputStream to SAP XmlPayload type, which is necessary to perform lookup request
			xmlRequest = LookupService.getXmlPayload(is);
		} catch (IOException ioe) {
			getTrace().addWarning("Encountered error during String to XmlPayload conversion while lookup ", ioe);
			return;
		}

		// Get lookup channel instance that provides access to ICo service
		Channel lookupChannel = LookupService.getChannel(LOOKUP_SERVICE, LOOKUP_CHANNEL);

		if (lookupChannel == null) {
			getTrace().addWarning("Could not get Communication Channel to perform ICo lookup, terminating.");
			return;
		}

		// Perform SOAP service call and clean up after done
		SystemAccessor sa = LookupService.getSystemAccessor(lookupChannel);
		xmlResponse = (XmlPayload) sa.call(xmlRequest);
		sa.close();

		if (xmlResponse == null) {
			getTrace().addWarning("Lookup did not return valid answer, can not collect all possible receiver systems.");
			return;
		}

		try (InputStream is = xmlResponse.getContent()) {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(is);

			if (doc.hasChildNodes()) {
				// 'MappingParamters' is not my mistake, it's typo in SAP's API.
				NodeList mappingParameters = Optional.ofNullable(doc.getElementsByTagName("MappingParamters"))
						.map(chldrn -> chldrn.item(0))
						.map(Node::getChildNodes)
						.orElse(null);

				if (mappingParameters == null) {
					getTrace().addWarning("Could not retrieve MappingParameters element, can not collect " +
							"all possible receiver systems.");
					return;
				}

				List<Node> mappingParametersList = IntStream.range(0, mappingParameters.getLength())
						.mapToObj(mappingParameters::item)
						.collect(Collectors.toList());

				mappingParametersList.forEach(node -> {
					if (node.getNodeType() == Node.ELEMENT_NODE) {
						Element mappingParameter = (Element) node;

						String bukrs = getTextContentFromElementTag(mappingParameter, "Name");
						String systemId = getTextContentFromElementTag(mappingParameter, "Value");

						if (!isNullOrEmpty(bukrs) && !isNullOrEmpty(systemId) && !receivers.containsKey(bukrs)) {
							receivers.put(bukrs, systemId);
							getTrace().addDebugMessage("Added receiver pair from lookup: '" + bukrs + "'-'" + systemId + "'");
						}
					}
				});

				getTrace().addDebugMessage("Parameters lookup is finished. ICo lookup returned " + mappingParameters.getLength() +
						" operation mapping parameters.");
			}
		} catch (IOException e) {
			getTrace().addWarning("Encountered IOException during reading lookup content ", e);
		} catch (ParserConfigurationException pce) {
			getTrace().addWarning("Encountered ParserConfigurationException during reading lookup content ", pce);
		} catch (SAXException se) {
			getTrace().addWarning("Encountered SAXException during reading lookup content ", se);
		}
	}

	/**
	 * Method adds collected receiver systems and company codes pairs
	 * to {@link DynamicConfiguration} object for further processing.
	 *
	 * @param dc - DynamicConfiguration object from SAP PO runtime
	 */
	private void addReceiversToDynamicConfiguration(DynamicConfiguration dc) {
		receivers.forEach((bukrs, systemId) -> {
			DynamicConfigurationKey bukrsKey =
					DynamicConfigurationKey.create(dcKeyNamespace, "R" + bukrs);
			dc.put(bukrsKey, systemId);
			getTrace().addDebugMessage("Put new pair to DynamicConfiguration. Key: " + bukrs + ", value: " + systemId);
		});
	}

	/**
	 * Method loads mapping parameters from properties file in mapping resources.
	 * Will return {@code false}, if any error appear.
	 *
	 * @return boolean indicator of operation success
	 */
	private boolean loadProperties() {

		// Mapping properties handler object
		RouterPropertiesHandler propHandler = RouterPropertiesHandler.getInstance();

		LOOKUP_SERVICE = propHandler.getPropertyValue("lookup.sappo.component.name");
		if (LOOKUP_SERVICE == null) {
			getTrace().addWarning("Can't load Communication Component property from 'router.properties' file.");
			return false;
		} else {
			getTrace().addDebugMessage("Loaded Communication Component property with value: '"
					+ LOOKUP_SERVICE + "' successfully");
		}

		LOOKUP_CHANNEL = propHandler.getPropertyValue("lookup.sappo.channel.ico.name");
		if (LOOKUP_CHANNEL == null) {
			getTrace().addWarning("Can't load Communication Channel property from 'router.properties' file.");
			return false;
		} else {
			getTrace().addDebugMessage("Loaded Communication Channel property with value: '"
					+ LOOKUP_CHANNEL + "' successfully");
		}

		MANAGEMENT_INFOTYPES = propHandler.getListPropertyValue("management.infotypes");
		if (MANAGEMENT_INFOTYPES == null) {
			getTrace().addWarning("Can't load Management Infotypes property from 'router.properties' file.");
			return false;
		} else {
			getTrace().addDebugMessage("Loaded Management Infotypes property with value: '"
					+ Arrays.toString(MANAGEMENT_INFOTYPES.toArray()) + "' successfully");
		}

		return true;
	}

	/**
	 * Method parses incoming message from {@link TransformationInput}
	 * to DOM {@link Document} or returns {@code null} if any error appear.
	 *
	 * @param ti {@link TransformationInput} object instance
	 *
	 * @return {@link Document} that contains incoming IDoc XML
	 * or {@code null} if any error appear.
	 */
	private Document getDocumentFromTransformationInput(TransformationInput ti) {
		getTrace().addDebugMessage("Started to parse HRMD_A09 XML to DOM Document.");
		try (InputStream is = ti.getInputPayload().getInputStream()){
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(is);
			getTrace().addDebugMessage("Finished parsing of HRMD_A09 XML to DOM Document.");
			return doc;
		} catch (IOException ioe) {
			getTrace().addWarning("Encountered IOException during incoming message parsing ", ioe);
		} catch (ParserConfigurationException pce) {
			getTrace().addWarning("Encountered ParserConfigurationException during incoming message parsing ", pce);
		} catch (SAXException se) {
			getTrace().addWarning("Encountered SAXException during incoming message parsing ", se);
		}
		return null;
	}

	/**
	 * Method generates XML {@link String} of SAP BASIS type {@code ReceiverDetermination}.
	 *
	 * @return {@link String} message with all Business Systems to which message should be routed
	 * or {@code null}, if {@link #receivers} {@link Map} is empty.
	 */
	private String generateReceiversXML() {
		if (receivers.isEmpty()) {
			getTrace().addWarning("Got empty receivers map - can't generate ReceiverDetermination XML.");
			return null;
		}

		Set<String> uniqueSystemIds = new HashSet<>(receivers.values());

		getTrace().addDebugMessage("Creating ReceiverDetermination XML. Final receivers count: " + uniqueSystemIds.size());

		StringBuilder sb = new StringBuilder();
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
		sb.append("<ns1:Receivers xmlns:ns1=\"http://sap.com/xi/XI/System\">");

		for (String systemId : uniqueSystemIds) {
			if (systemId == null || "".equals(systemId)) continue;

			sb.append("<Receiver><Service>");
			sb.append(systemId);
			sb.append("</Service></Receiver>");
		}

		sb.append("</ns1:Receivers>");

		return sb.toString();
	}

	/**
	 * Method writes XML message string of SAP BASIS type {@code ReceiverDetermination}
	 * into {@link OutputStream} in {@link TransformationOutput} object instance.
	 *
	 * @param to - {@link TransformationOutput} object instance
	 * @param xml - target message xml string payload
	 */
	private void writeXMLToTransformationOutput(TransformationOutput to, String xml) {
		try (OutputStream os = to.getOutputPayload().getOutputStream()) {
			os.write(xml.getBytes(StandardCharsets.UTF_8));
			getTrace().addDebugMessage("Finished writing result message to 'TransformationOutput'");
		} catch (Exception e) {
			getTrace().addWarning("Encountered error during writing to TransformationOutput ", e);
		}
	}

	/**
	 * Utility method to get text content from tag inside given {@link Element}.
	 *
	 * @param element element to walk through
	 * @param tagName name of tag with target text content
	 *
	 * @return {@link String} with text value of given {@link Element}
	 */
	private String getTextContentFromElementTag(Element element, String tagName) {
		return Optional.ofNullable(element.getElementsByTagName(tagName))
				.map(chldrn -> chldrn.item(0))
				.map(Node::getTextContent)
				.orElse("");
	}

	/**
	 * Utility method to check if {@link String} is null or empty.
	 *
	 * @param string input String to check
	 *
	 * @return boolean result of check
	 */
	private boolean isNullOrEmpty(String string) {
		return string == null || string.length() == 0;
	}

}
