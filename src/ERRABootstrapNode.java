// Il programma viene fatto partire con "java ERRABootstrapNode"

import java.net.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;
import java.util.Scanner;

public class ERRABootstrapNode {
	private static final int PKT_LENGTH = 40000;
	private static final int CONNECTION_CODE = 251;
	private static final int DISCONNECTION_CODE = 252;
	private static final int ADDRESSES_LIST_REQUEST_CODE = 253;
	private static final int BOOTSTRAP_RESPONSE_SUCCESS_CODE = 254;
	private static final int BOOTSTRAP_RESPONSE_ERROR_CODE = 255;

	private static InetAddress myIP = null;
	private static int numberMSGSent = 0;
	private static HashMap<String, byte[]> MSGList = new HashMap<String, byte[]>();
	private static ArrayList<InetAddress> addressesList = new ArrayList<InetAddress>();

	public static void main(String[] args) {
		PacketListener pl = new PacketListener();
		pl.setDaemon(true);
		pl.start();
		waitForMessage();
	}
	
	private static void waitForMessage() {
		String name = "";
		Scanner in = new Scanner(System.in);

		while (true) {
			System.out.print("\nName of file to send (/logout to disconnect): ");
			name = in.nextLine();
			if (name.equals("/logout"))
				break;
			sendFile(name);
		}

		in.close();
	}

	private static void sendFile(String fileName) {
		numberMSGSent++;

		Socket outputSocket = null;
		try {
			InetAddress destination = InetAddress.getByName(fileName);
			if (!addressesList.contains(destination)) {
				System.out.println("The specified destination is not connected to the ERRA network");
				return;
			}

			ArrayList<InetAddress> addressesList2 = new ArrayList<InetAddress>(addressesList);
			addressesList2.remove(myIP);
			addressesList2.remove(destination);

			ArrayList<File> pktMSG = new ArrayList<File>();
			File f = new File(fileName);
			long MSGLength = f.length();
			BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
			FileOutputStream out;
			byte partCounter = 0;
			byte[] buffer = new byte[PKT_LENGTH];
			int tmp = 0;
			while ((tmp = bis.read(buffer)) > 0) {   // split del file
				File newFile = new File(fileName + "." + String.format("%03d", partCounter & 0xFF));
				newFile.createNewFile();
				out = new FileOutputStream(newFile);
				out.write(buffer,0,tmp);
				pktMSG.add(newFile);
				partCounter++;
				out.close();
			}
			bis.close();

			for (File pkt : pktMSG) {

				String pktName = pkt.getName(); 
				short numberPKT = Short.parseShort(pktName.split("\\.")[4]);  // il file ha come nome "a.b.c.d.e", "e" è quello che ci serve

				Collections.shuffle(addressesList2, new Random(System.nanoTime()));

				outputSocket = null;
				int i = 0;
				while (outputSocket == null) {
					InetAddress nextNode = null;
					try {
						if (i >= addressesList2.size()) {
							System.out.println("Lista nodi finita");
							return;
						}
						nextNode = addressesList2.get(i);
						outputSocket = new Socket(nextNode, 10000);
					} catch (ConnectException e) {
						if (addressesList.contains(nextNode)) {
							addressesList.remove(nextNode);
							System.out.println("Node with address " +  nextNode.getHostAddress() + " removed from the network");
						}
					}
					i++;
				}

				try {
					DataOutputStream output = new DataOutputStream(new BufferedOutputStream(outputSocket.getOutputStream(), PKT_LENGTH + 15 + 4*addressesList2.size()));
					output.writeByte(addressesList2.size() - i + 1);
					output.write(myIP.getAddress(), 0, 4); 
					output.writeByte(numberMSGSent);
					output.writeByte(numberPKT);
					output.writeLong(MSGLength);

					for (; i < addressesList2.size(); i++) 
						output.write(addressesList2.get(i).getAddress(), 0, 4);   // ogni indirizzo viene mandato come gruppo di 4 byte
					output.write(destination.getAddress(), 0, 4);   // mette in fondo alla lista l'indirizzo della destinazione

					byte[] buffer2 = new byte[PKT_LENGTH];
					int len;
					FileInputStream fis = new FileInputStream(pkt);
					while ((len = fis.read(buffer2)) > 0)
						output.write(buffer2, 0, len);

					output.flush();
					fis.close();
				} catch (IOException e) {
					System.err.println("Problem sending file");
				} finally {
					try {
						if (outputSocket != null)
							outputSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		} catch (IOException e) {
			System.err.println("Problem sending file");
		} finally {
			try {
				if (outputSocket != null)
					outputSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static class PacketListener extends Thread {

		public void run() {
			ServerSocket server = null;
			try {
				server = new ServerSocket(10000);
			} catch (IOException e) {
				System.err.println("Problem creating this node's server");
			}        

			while (true) {
				System.out.println("Waiting for packets");
				Socket inputSocket = null;
				try {
					inputSocket = server.accept();
					DataInputStream input = new DataInputStream(new BufferedInputStream(inputSocket.getInputStream(), PKT_LENGTH + 15 + 4*256));
					byte firstByte = input.readByte();
					switch (firstByte & 0xFF) {      // conversione a byte senza segno
						case 0:
							saveFileFragment(input);
							break;
						case CONNECTION_CODE:
							addNode(inputSocket);
							break;
						case DISCONNECTION_CODE:
							removeNode(inputSocket, input);
							break;
						case ADDRESSES_LIST_REQUEST_CODE:
							sendAddressesList(input, inputSocket);
							break;
						case BOOTSTRAP_RESPONSE_SUCCESS_CODE:
							System.err.println("This should never happen");  
							break;
						case BOOTSTRAP_RESPONSE_ERROR_CODE:
							System.err.println("This should never happen");  
							break;
						default:
							forwardPacket(input, firstByte);
							break;
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						inputSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}

		private void forwardPacket(DataInputStream input, byte remainingNodesNum) {
			Socket outputSocket = null;
			try {
				// leggo tutte le varie parti dell'header separatamente solo perché mi serve l'id del messaggio e il numero di pacchetto
				// da stampare sul terminale
				byte[] sourceAddress = new byte[4];
				input.read(sourceAddress);
				byte numberMSGSent = input.readByte();
				int numberOfReceivedPkt = input.readByte() & 0xFF;
				long MSGLength = input.readLong();
				
				String idMSG = InetAddress.getByAddress(sourceAddress).getHostAddress() + "." + numberMSGSent;
				
				byte[] nextHopAddress = new byte[4];
				
				while (outputSocket == null) {
					try {
						input.read(nextHopAddress);
						outputSocket = new Socket(InetAddress.getByAddress(nextHopAddress), 10000);
					} catch (ConnectException e) {
						addressesList.remove(InetAddress.getByAddress(nextHopAddress));
						remainingNodesNum--;
					}
				}
				
				DataOutputStream output = new DataOutputStream(new BufferedOutputStream(outputSocket.getOutputStream(), PKT_LENGTH + 15 + 4*(remainingNodesNum - 1)));
				output.writeByte(remainingNodesNum - 1);
				output.write(sourceAddress, 0, 4); 
				output.writeByte(numberMSGSent);
				output.writeByte(numberOfReceivedPkt);
				output.writeLong(MSGLength);

				byte[] buffer = null;
				if (numberOfReceivedPkt == MSGLength/PKT_LENGTH) {   // se il pacchetto è l'ultimo
					buffer = new byte[(int)(MSGLength%PKT_LENGTH) + 4*(remainingNodesNum - 1)];
				} else {
					buffer = new byte[PKT_LENGTH + 4*(remainingNodesNum - 1)];
				}
				input.readFully(buffer);
				output.write(buffer, 0, buffer.length);
				output.flush();
				System.out.println("Packet " + numberOfReceivedPkt + " of message " + idMSG + " forwarded (remaining nodes: " + (remainingNodesNum & 0xFF) + ")");
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (outputSocket != null)
						outputSocket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		
		private void saveFileFragment(DataInputStream input) {
			try {
				byte[] sourceAddress = new byte[4];
				input.read(sourceAddress);
				byte numberMSGSent = input.readByte();
				int numberOfReceivedPkt = input.readByte() & 0xFF;
				long MSGLength = input.readLong();
				String idMSG = InetAddress.getByAddress(sourceAddress).getHostAddress() + "." + numberMSGSent;
				System.out.println("Packet " + numberOfReceivedPkt + " of message " + idMSG + " reached the destination");

				if (!MSGList.containsKey(idMSG)) {
					byte[] tmp = new byte[(int) (1 + MSGLength)];
					tmp[0] = (byte) Math.ceil((((double)(MSGLength))/PKT_LENGTH));
					MSGList.put(idMSG, tmp);
				}
				int offset = 1 + PKT_LENGTH * numberOfReceivedPkt;
				byte[] buffer = null;
				if (numberOfReceivedPkt == MSGLength/PKT_LENGTH) {   // se il pacchetto è l'ultimo
					buffer = new byte[(int)(MSGLength%PKT_LENGTH)];
				} else {
					buffer = new byte[PKT_LENGTH];
				}
				input.readFully(buffer);
				System.arraycopy(buffer, 0, MSGList.get(idMSG), offset, buffer.length);

				byte[] tmp1 = MSGList.get(idMSG);
				if (--tmp1[0] == 0) {    // se abbiamo tutti i pacchetti
					saveFile(tmp1, idMSG);
					MSGList.remove(idMSG);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void saveFile(byte[] tmp, String idMSG) {
			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(idMSG);
				fos.write(tmp, 1, tmp.length - 1);

				System.out.println("File saved as " + idMSG);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fos != null)
						fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}			
		}

		private void addNode(Socket inputSocket) throws IOException {
			InetAddress newNodeAddress = inputSocket.getInetAddress();
			if (addressesList.isEmpty()) {
				myIP = inputSocket.getLocalAddress();
				addressesList.add(myIP);  // il primo nodo che si connette alla rete permette al bootstrap
																	// di conoscere il proprio indirizzo IP, che viene ricavato
																	// dal socket della connessione
			}
			addressesList.add(newNodeAddress);
			System.out.println("Node with address " +  newNodeAddress.getHostAddress() + " added to the network");
		}

		private void removeNode(Socket inputSocket, DataInputStream input) throws IOException {
			byte[] removedNodeAddress = new byte[4];
			input.read(removedNodeAddress);
			InetAddress removedNodeAddress2 = InetAddress.getByAddress(removedNodeAddress);
			if (!addressesList.contains(removedNodeAddress2)) {
				return;
			}
			addressesList.remove(removedNodeAddress2);
			System.out.println("Node with address " +  removedNodeAddress2.getHostAddress() + " removed from the network");

		}

		private void sendAddressesList(DataInputStream input, Socket inputSocket) throws IOException {
			byte[] destinationAddress = new byte[4];

			input.read(destinationAddress);
			InetAddress destination = InetAddress.getByAddress(destinationAddress);
			InetAddress source = inputSocket.getInetAddress();

			DataOutputStream output = new DataOutputStream(new BufferedOutputStream(inputSocket.getOutputStream(), 1 + 4*addressesList.size()));
			if (!addressesList.contains(destination)) {
				output.write(BOOTSTRAP_RESPONSE_ERROR_CODE);
				output.flush();
				return;
			}
			System.out.println("Sending addresses list to node " + source.getHostAddress());
			output.write(BOOTSTRAP_RESPONSE_SUCCESS_CODE);

			// mette nella lista tutti gli indirizzi tranne quello della sorgente e quello della destinazione
			for (InetAddress address : addressesList) {
				if (!address.equals(source) && !address.equals(destination))
					output.write(address.getAddress(), 0, 4);
			}
			output.flush();
		}

	}

}