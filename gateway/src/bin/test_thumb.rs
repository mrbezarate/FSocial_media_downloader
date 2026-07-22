use teloxide::types::{InputMedia, InputMediaAudio, InputFile};

fn main() {
    let _ = InputMediaAudio::new(InputFile::file("foo.mp3")).thumbnail(InputFile::file("thumb.jpg"));
}
