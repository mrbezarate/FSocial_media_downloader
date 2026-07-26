use teloxide::types::{InputFile, InputMediaAudio};

fn main() {
    let _ =
        InputMediaAudio::new(InputFile::file("foo.mp3")).thumbnail(InputFile::file("thumb.jpg"));
}
