#![allow(non_camel_case_types)]
use libp2p_server::{P2PServer, ClientBehaviour};

use ark_bls12_381::Bls12_381;
use ark_poly_commit::marlin_pc::MarlinKZG10;
use ark_crypto_primitives::sponge::poseidon::PoseidonSponge;
use ark_ec::pairing::Pairing;

use ark_poly::{univariate::DensePolynomial, DenseUVPolynomial};


/* 
Reliable Broadcast
    - Sender sends a message to all peers it is connected to
    - Each peer sends the message, if it has not already seen it, to all peers it is connected to
    - Once a peer receives 1/3 of the messages, it sends an acknowledgement to all peers it is connected to
    - Once a peer receives 2/3 of the acknowledgements, it sends a commit message to all peers it is connected to
    - Once a peer receives 2/3 of the commit messages, it commits the message
*/
type UniPoly_381 = DensePolynomial<<Bls12_381 as Pairing>::ScalarField>;
type Sponge_Bls12_381 = PoseidonSponge<<Bls12_381 as Pairing>::ScalarField>;
type PCS = MarlinKZG10<Bls12_381, UniPoly_381, Sponge_Bls12_381>;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> { 

    // Run the server
    P2PServer::initialize_server(Some("./src/bootstrap-nodes.txt".to_string()), ClientBehaviour::VabaBroadcast).await?;


    Ok(())
}
